#include <cerrno>
#include <chrono>
#include <csignal>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>

#include <boost/any/bad_any_cast.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>

#include <spider/core/Driver.hpp>
#include <spider/core/Error.hpp>
#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/scheduler/FifoPolicy.hpp>
#include <spider/scheduler/SchedulerPolicy.hpp>
#include <spider/scheduler/SchedulerServer.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/mysql/MySqlStorageFactory.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>
#include <spider/utils/env.hpp>
#include <spider/utils/logging.hpp>
#include <spider/utils/StopFlag.hpp>

constexpr int cCmdArgParseErr = 1;
constexpr int cSignalHandleErr = 2;
constexpr int cStorageConnectionErr = 3;
constexpr int cSchedulerAddrErr = 4;
constexpr int cStorageErr = 5;

constexpr int cCleanupInterval = 1000;
constexpr int cRetryCount = 5;

namespace {
/*
 * Signal handler for SIGTERM. Sets the stop flag to request a stop.
 * @param signal The signal number.
 */
auto stop_scheduler_handler(int signal) -> void {
    if (SIGTERM == signal) {
        spider::core::StopFlag::request_stop();
    }
}

auto parse_args(int const argc, char** argv) -> boost::program_options::variables_map {
    boost::program_options::options_description desc;
    desc.add_options()("help", "spider scheduler");
    desc.add_options()(
            "host",
            boost::program_options::value<std::string>(),
            "scheduler host address"
    );
    desc.add_options()(
            "port",
            boost::program_options::value<unsigned short>(),
            "port to listen on"
    );
    desc.add_options()(
            "storage_url",
            boost::program_options::value<std::string>(),
            "storage server url"
    );

    boost::program_options::variables_map variables;
    boost::program_options::store(
            // NOLINTNEXTLINE(misc-include-cleaner)
            boost::program_options::parse_command_line(argc, argv, desc),
            variables
    );
    boost::program_options::notify(variables);
    return variables;
}

auto heartbeat_loop(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        spider::core::Scheduler const& scheduler
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
                = metadata_store->update_heartbeat(*conn, scheduler.get_id());
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

auto cleanup_loop(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::DataStorage> const& data_store
) -> void {
    while (!spider::core::StopFlag::is_stop_requested()) {
        std::this_thread::sleep_for(std::chrono::seconds(cCleanupInterval));
        spdlog::debug("Starting cleanup");
        std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
                conn_result = storage_factory->provide_storage_connection();
        if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<spider::core::StorageErr>(conn_result).description
            );
            continue;
        }
        auto conn = std::move(
                std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result)
        );

        data_store->remove_dangling_data(*conn);
        spdlog::debug("Finished cleanup");
    }
}

constexpr int cSignalExitBase = 128;
}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
auto main(int argc, char** argv) -> int {
    spider::utils::setup_file_logger("spider_scheduler", "spider.scheduler");

    boost::program_options::variables_map const args = parse_args(argc, argv);

    unsigned short port = 0;
    std::string scheduler_addr;
    std::string storage_url;
    try {
        if (!args.contains("port")) {
            spdlog::error("port is required");
            return cCmdArgParseErr;
        }
        port = args["port"].as<unsigned short>();
        if (!args.contains("host")) {
            spdlog::error("host is required");
            return cCmdArgParseErr;
        }
        scheduler_addr = args["host"].as<std::string>();

        auto const optional_storage_url_env = spider::utils::get_env(spider::utils::cStorageUrlEnv);
        if (optional_storage_url_env.has_value()) {
            storage_url = optional_storage_url_env.value();
        } else if (args.contains("storage_url")) {
            spdlog::warn(
                    "Prefer using `{}` environment variable over `--storage_url` argument.",
                    spider::utils::cStorageUrlEnv
            );
            storage_url = args["storage_url"].as<std::string>();
        } else {
            spdlog::error(
                    "Storage URL must be provided via `{}` environment variable or `--storage_url` "
                    "argument.",
                    spider::utils::cStorageUrlEnv
            );
            return cCmdArgParseErr;
        }
    } catch (boost::bad_any_cast& e) {
        return cCmdArgParseErr;
    } catch (boost::program_options::error& e) {
        return cCmdArgParseErr;
    }

    // Ignore SIGTERM
    // NOLINTBEGIN(misc-include-cleaner)
    struct sigaction sig_action{};
    sig_action.sa_handler = stop_scheduler_handler;
    sigemptyset(&sig_action.sa_mask);
    sig_action.sa_flags |= SA_RESTART;
    if (0 != sigaction(SIGTERM, &sig_action, nullptr)) {
        spdlog::error("Fail to install signal handler for SIGTERM: errno {}", errno);
        return cSignalHandleErr;
    }
    // NOLINTEND(misc-include-cleaner)

    // Create storages
    std::shared_ptr<spider::core::StorageFactory> const storage_factory
            = std::make_unique<spider::core::MySqlStorageFactory>(storage_url);
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = storage_factory->provide_metadata_storage();
    std::shared_ptr<spider::core::DataStorage> const data_store
            = storage_factory->provide_data_storage();

    // Initialize storages
    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
        spdlog::error(
                "Failed to connection to storage: {}",
                std::get<spider::core::StorageErr>(conn_result).description
        );
    }
    std::shared_ptr<spider::core::StorageConnection> const conn
            = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    spider::core::StorageErr err = metadata_store->initialize(*conn);
    if (!err.success()) {
        spdlog::error("Failed to initialize metadata storage: {}", err.description);
        return cStorageErr;
    }
    err = data_store->initialize(*conn);
    if (!err.success()) {
        spdlog::error("Failed to initialize data storage: {}", err.description);
        return cStorageErr;
    }

    // Get scheduler id and addr
    boost::uuids::random_generator gen;
    boost::uuids::uuid const scheduler_id = gen();

    // Register scheduler with storage
    spider::core::Scheduler const scheduler{scheduler_id, scheduler_addr, port};
    err = metadata_store->add_scheduler(*conn, scheduler);
    if (!err.success()) {
        spdlog::error("Failed to register scheduler with storage server: {}", err.description);
        return cStorageErr;
    }

    // Start scheduler server
    std::shared_ptr<spider::scheduler::SchedulerPolicy> const policy
            = std::make_shared<spider::scheduler::FifoPolicy>(
                    scheduler_id,
                    metadata_store,
                    data_store,
                    conn
            );
    spider::scheduler::SchedulerServer server{port, policy, metadata_store, data_store, conn};

    try {
        // Start a thread that periodically updates the scheduler's heartbeat
        std::thread heartbeat_thread{
                heartbeat_loop,
                std::cref(storage_factory),
                std::cref(metadata_store),
                std::ref(scheduler)
        };

        // Start a thread that periodically starts cleanup
        std::thread cleanup_thread{cleanup_loop, std::cref(storage_factory), std::cref(data_store)};

        heartbeat_thread.join();
        cleanup_thread.join();
        server.stop();
    } catch (std::system_error& e) {
        spdlog::error("Failed to join thread: {}", e.what());
    }

    // If SIGTERM was caught and StopFlag is requested, set the exit value corresponding to SIGTERM.
    if (spider::core::StopFlag::is_stop_requested()) {
        return cSignalExitBase + SIGTERM;
    }

    metadata_store->remove_driver(*conn, scheduler_id);

    return 0;
}
