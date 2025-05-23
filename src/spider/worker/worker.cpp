#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/any/bad_any_cast.hpp>
#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>  // IWYU pragma: keep
#include <spdlog/spdlog.h>

#include <spider/core/Data.hpp>
#include <spider/core/Driver.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/Task.hpp>
#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/io/Serializer.hpp>  // IWYU pragma: keep
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/mysql/MySqlStorageFactory.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>
#include <spider/utils/StopFlag.hpp>
#include <spider/worker/ChildPid.hpp>
#include <spider/worker/TaskExecutor.hpp>
#include <spider/worker/WorkerClient.hpp>

constexpr int cCmdArgParseErr = 1;
constexpr int cSignalHandleErr = 2;
constexpr int cWorkerAddrErr = 3;
constexpr int cStorageConnectionErr = 4;
constexpr int cStorageErr = 5;
constexpr int cTaskErr = 6;

constexpr int cRetryCount = 5;

namespace {
/*
 * Signal handler for SIGTERM. It sets the stop flag to request a stop and sends SIGTERM to the task
 * executor.
 * @param signal The signal number.
 */
auto stop_task_handler(int signal) -> void {
    if (SIGTERM == signal) {
        spider::core::StopFlag::request_stop();
        // Send SIGTERM to task executor
        pid_t const pid = spider::core::ChildPid::get_pid();
        if (pid > 0) {
            // NOLINTNEXTLINE(misc-include-cleaner)
            kill(pid, SIGTERM);
        }
    }
}

auto parse_args(int const argc, char** argv) -> boost::program_options::variables_map {
    boost::program_options::options_description desc;
    desc.add_options()("help", "spider scheduler");
    desc.add_options()(
            "storage_url",
            boost::program_options::value<std::string>(),
            "storage server url"
    );
    desc.add_options()(
            "libs",
            boost::program_options::value<std::vector<std::string>>(),
            "dynamic libraries that include the spider tasks"
    );
    desc.add_options()("host", boost::program_options::value<std::string>(), "worker host address");

    boost::program_options::variables_map variables;
    boost::program_options::store(
            // NOLINTNEXTLINE(misc-include-cleaner)
            boost::program_options::parse_command_line(argc, argv, desc),
            variables
    );
    boost::program_options::notify(variables);
    return variables;
}

auto get_environment_variable() -> absl::flat_hash_map<
        boost::process::v2::environment::key,
        boost::process::v2::environment::value> {
    boost::filesystem::path const executable_dir = boost::dll::program_location().parent_path();

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char const* path_env_str = std::getenv("PATH");
    std::string path_env = nullptr == path_env_str ? "" : path_env_str;
    path_env.append(":");
    path_env.append(executable_dir.string());

    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value>
            environment_variables;

    environment_variables.emplace("PATH", path_env);

    return environment_variables;
}

auto heartbeat_loop(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        spider::core::Driver const& driver
) -> void {
    int fail_count = 0;
    while (!spider::core::StopFlag::is_stop_requested()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        spdlog::debug("Updating heartbeat");
        std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
                conn_result = storage_factory->provide_storage_connection();
        if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<spider::core::StorageErr>(conn_result).description
            );
            fail_count++;
            continue;
        }
        auto conn = std::move(
                std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result)
        );

        spider::core::StorageErr const err
                = metadata_store->update_heartbeat(*conn, driver.get_id());
        if (!err.success()) {
            spdlog::error("Failed to update scheduler heartbeat: {}", err.description);
            fail_count++;
        } else {
            fail_count = 0;
        }
        if (fail_count >= cRetryCount - 1) {
            spider::core::StopFlag::request_stop();
            break;
        }
    }
}

constexpr int cFetchTaskTimeout = 100;

auto
fetch_task(spider::worker::WorkerClient& client, std::optional<boost::uuids::uuid> fail_task_id)
        -> std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> {
    spdlog::debug("Fetching task");
    while (!spider::core::StopFlag::is_stop_requested()) {
        std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> const optional_task_ids
                = client.get_next_task(fail_task_id);
        if (optional_task_ids.has_value()) {
            return optional_task_ids;
        }
        // If the first request succeeds, later requests should not include the failed task id
        fail_task_id = std::nullopt;
        std::this_thread::sleep_for(std::chrono::milliseconds(cFetchTaskTimeout));
    }
    return std::nullopt;
}

/*
 * Sets up a task by fetching the task metadata from storage and creating argument buffers from task
 * inputs.
 *
 * @param storage_factory The storage factory to create a storage connection.
 * @param metadata_store The metadata storage to fetch task details.
 * @param instance The task instance to set up.
 * @param task Output parameter to store the fetched task details.
 * @return  A vector of buffers containing the serialized arguments of the task.
 * @return std::nullopt if any failure occurs.
 */
auto setup_task(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        spider::core::TaskInstance const& instance,
        spider::core::Task& task
) -> std::optional<std::vector<msgpack::sbuffer>> {
    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
        spdlog::error(
                "Failed to connect to storage: {}",
                std::get<spider::core::StorageErr>(conn_result).description
        );
        return std::nullopt;
    }
    std::unique_ptr<spider::core::StorageConnection> conn
            = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    // Get task details
    spider::core::StorageErr const err = metadata_store->get_task(*conn, instance.task_id, &task);
    if (!err.success()) {
        spdlog::error("Failed to fetch task detail: {}", err.description);
        return std::nullopt;
    }

    std::optional<std::vector<msgpack::sbuffer>> optional_arg_buffers = task.get_arg_buffers();
    if (!optional_arg_buffers.has_value()) {
        spdlog::error("Failed to fetch task arguments");
        metadata_store->task_fail(*conn, instance, fmt::format("Failed to fetch task arguments"));
        return std::nullopt;
    }
    return optional_arg_buffers;
}

auto
parse_outputs(spider::core::Task const& task, std::vector<msgpack::sbuffer> const& result_buffers)
        -> std::optional<std::vector<spider::core::TaskOutput>> {
    std::vector<spider::core::TaskOutput> outputs;
    outputs.reserve(task.get_num_outputs());
    for (size_t i = 0; i < task.get_num_outputs(); ++i) {
        std::string const type = task.get_output(i).get_type();
        if (type == typeid(spider::core::Data).name()) {
            try {
                msgpack::object_handle const handle
                        = msgpack::unpack(result_buffers[i].data(), result_buffers[i].size());
                msgpack::object const obj = handle.get();
                boost::uuids::uuid data_id;
                obj.convert(data_id);
                outputs.emplace_back(data_id);
            } catch (std::runtime_error const& e) {
                spdlog::error(
                        "Task {} failed to parse result as data id",
                        task.get_function_name()
                );
                return std::nullopt;
            }
        } else {
            msgpack::sbuffer const& buffer = result_buffers[i];
            std::string const value{buffer.data(), buffer.size()};
            outputs.emplace_back(value, type);
        }
    }
    return outputs;
}

/**
 * Handles the result of a task execution. Parse the task outputs and submit them to the storage.
 *
 * @param storage_factory Factory for creating storage connections.
 * @param metadata_store Metadata storage for submitting results.
 * @param instance Task instance that was executed.
 * @param task The task that was executed.
 * @param executor The executor that ran the task.
 * @return true if results were successfully handled, false if any errors occurred.
 */
auto handle_executor_result(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        spider::core::TaskInstance const& instance,
        spider::core::Task const& task,
        spider::worker::TaskExecutor& executor
) -> bool {
    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
        spdlog::error(
                "Failed to connect to storage: {}",
                std::get<spider::core::StorageErr>(conn_result).description
        );
        return false;
    }
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    if (!executor.succeed()) {
        spdlog::warn("Task {} failed", task.get_function_name());
        metadata_store->task_fail(
                *conn,
                instance,
                fmt::format("Task {} failed", task.get_function_name())
        );
        return false;
    }

    // Parse result
    std::optional<std::vector<msgpack::sbuffer>> const optional_result_buffers
            = executor.get_result_buffers();
    if (!optional_result_buffers.has_value()) {
        spdlog::error("Task {} failed to parse result into buffers", task.get_function_name());
        metadata_store->task_fail(
                *conn,
                instance,
                fmt::format("Task {} failed to parse result into buffers", task.get_function_name())
        );
        return false;
    }
    std::vector<msgpack::sbuffer> const& result_buffers = optional_result_buffers.value();
    std::optional<std::vector<spider::core::TaskOutput>> const optional_outputs
            = parse_outputs(task, result_buffers);
    if (!optional_outputs.has_value()) {
        metadata_store->task_fail(
                *conn,
                instance,
                fmt::format(
                        "Task {} failed to parse result into TaskOutput",
                        task.get_function_name()
                )
        );
        return false;
    }

    std::vector<spider::core::TaskOutput> const& outputs = optional_outputs.value();
    // Submit result
    spdlog::debug("Submitting result for task {}", boost::uuids::to_string(task.get_id()));
    spider::core::StorageErr err;
    for (int i = 0; i < cRetryCount; ++i) {
        err = metadata_store->task_finish(*conn, instance, outputs);
        if (err.success()) {
            break;
        }
        if (spider::core::StorageErrType::DeadLockErr != err.type) {
            spdlog::error("Submit task {} fails: {}", task.get_function_name(), err.description);
            break;
        }
    }
    if (!err.success()) {
        spdlog::error("Submit task {} fails: {}", task.get_function_name(), err.description);
        return false;
    }
    return true;
}

// NOLINTBEGIN(clang-analyzer-unix.BlockInCriticalSection)
auto task_loop(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        spider::worker::WorkerClient& client,
        std::string const& storage_url,
        std::vector<std::string> const& libs,
        absl::flat_hash_map<
                boost::process::v2::environment::key,
                boost::process::v2::environment::value> const& environment
) -> void {
    std::optional<boost::uuids::uuid> fail_task_id = std::nullopt;
    while (!spider::core::StopFlag::is_stop_requested()) {
        boost::asio::io_context context;

        auto const& optional_task = fetch_task(client, fail_task_id);
        if (false == optional_task.has_value()) {
            continue;
        }
        auto const [task_id, task_instance_id] = optional_task.value();
        spider::core::TaskInstance const instance{task_instance_id, task_id};
        spdlog::debug("Fetched task {}", boost::uuids::to_string(task_id));
        // Fetch task detail from metadata storage
        spider::core::Task task{""};

        std::optional<std::vector<msgpack::sbuffer>> optional_arg_buffers
                = setup_task(storage_factory, metadata_store, instance, task);
        if (!optional_arg_buffers.has_value()) {
            spdlog::error("Failed to setup task {}", task.get_function_name());
            fail_task_id = task.get_id();
            continue;
        }
        std::vector<msgpack::sbuffer> const& arg_buffers = optional_arg_buffers.value();

        // Execute task
        spider::worker::TaskExecutor executor{
                context,
                task.get_function_name(),
                task.get_id(),
                storage_url,
                libs,
                environment,
                arg_buffers
        };

        pid_t const pid = executor.get_pid();
        spider::core::ChildPid::set_pid(pid);
        // Double check if stop token is set to avoid any missing signal
        if (spider::core::StopFlag::is_stop_requested()) {
            // NOLINTNEXTLINE(misc-include-cleaner)
            kill(pid, SIGTERM);
        }

        context.run();
        executor.wait();

        spider::core::ChildPid::set_pid(0);

        if (handle_executor_result(storage_factory, metadata_store, instance, task, executor)) {
            fail_task_id = std::nullopt;
        } else {
            fail_task_id = task.get_id();
        }
    }
}

// NOLINTEND(clang-analyzer-unix.BlockInCriticalSection)

constexpr int cSignalExitBase = 128;
}  // namespace

auto main(int argc, char** argv) -> int {
    // Set up spdlog to write to stderr
    // NOLINTNEXTLINE(misc-include-cleaner)
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [spider.worker] %v");
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#endif

    boost::program_options::variables_map const args = parse_args(argc, argv);

    std::string storage_url;
    std::vector<std::string> libs;
    std::string worker_addr;
    try {
        if (!args.contains("storage_url")) {
            spdlog::error("Missing storage_url");
            return cCmdArgParseErr;
        }
        storage_url = args["storage_url"].as<std::string>();
        if (!args.contains("host")) {
            spdlog::error("Missing host");
            return cCmdArgParseErr;
        }
        worker_addr = args["host"].as<std::string>();
        if (!args.contains("libs") || args["libs"].empty()) {
            spdlog::error("Missing libs");
            return cCmdArgParseErr;
        }
        libs = args["libs"].as<std::vector<std::string>>();
    } catch (boost::bad_any_cast const& e) {
        spdlog::error("Error: {}", e.what());
        return cCmdArgParseErr;
    } catch (boost::program_options::error const& e) {
        spdlog::error("Error: {}", e.what());
        return cCmdArgParseErr;
    }

    // NOLINTBEGIN(misc-include-cleaner)
    struct sigaction sig_action{};
    sig_action.sa_handler = stop_task_handler;
    sigemptyset(&sig_action.sa_mask);
    sig_action.sa_flags |= SA_RESTART;
    if (0 != sigaction(SIGTERM, &sig_action, nullptr)) {
        spdlog::error("Fail to install signal handler for SIGTERM: errno {}", errno);
        return cSignalHandleErr;
    }
    // NOLINTEND(misc-include-cleaner)

    // Create storage
    std::shared_ptr<spider::core::StorageFactory> const storage_factory
            = std::make_shared<spider::core::MySqlStorageFactory>(storage_url);
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = storage_factory->provide_metadata_storage();
    std::shared_ptr<spider::core::DataStorage> const data_store
            = storage_factory->provide_data_storage();

    boost::uuids::random_generator gen;
    boost::uuids::uuid const worker_id = gen();
    spider::core::Driver driver{worker_id};

    {  // Keep the scope of RAII storage connection
        std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
                conn_result = storage_factory->provide_storage_connection();
        if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<spider::core::StorageErr>(conn_result).description
            );
            return cStorageErr;
        }
        auto conn = std::move(
                std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result)
        );

        spider::core::StorageErr const err = metadata_store->add_driver(*conn, driver);
        if (!err.success()) {
            spdlog::error("Cannot add driver to metadata storage: {}", err.description);
            return cStorageErr;
        }
    }

    // Start client
    spider::worker::WorkerClient
            client{worker_id, worker_addr, data_store, metadata_store, storage_factory};

    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variables
            = get_environment_variable();

    // Start a thread that periodically updates the scheduler's heartbeat
    std::thread heartbeat_thread{
            heartbeat_loop,
            std::cref(storage_factory),
            std::cref(metadata_store),
            std::ref(driver),
    };

    // Start a thread that processes tasks
    std::thread task_thread{
            task_loop,
            std::cref(storage_factory),
            std::cref(metadata_store),
            std::ref(client),
            std::cref(storage_url),
            std::cref(libs),
            std::cref(environment_variables),
    };

    heartbeat_thread.join();
    task_thread.join();

    // If SIGTERM was caught and StopFlag is requested, set the exit value corresponding to SIGTERM.
    if (spider::core::StopFlag::is_stop_requested()) {
        return cSignalExitBase + SIGTERM;
    }

    return 0;
}
