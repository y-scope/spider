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
#include <spdlog/sinks/stdout_color_sinks.h>  // IWYU pragma: keep
#include <spdlog/spdlog.h>

#include "../core/Driver.hpp"
#include "../core/Error.hpp"
#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/mysql/MySqlStorageFactory.hpp"
#include "../storage/StorageConnection.hpp"
#include "../storage/StorageFactory.hpp"
#include "../utils/StopToken.hpp"
#include "FifoPolicy.hpp"
#include "SchedulerPolicy.hpp"
#include "SchedulerServer.hpp"

constexpr int cCmdArgParseErr = 1;
constexpr int cSignalHandleErr = 2;
constexpr int cStorageConnectionErr = 3;
constexpr int cSchedulerAddrErr = 4;
constexpr int cStorageErr = 5;

constexpr int cCleanupInterval = 1000;
constexpr int cRetryCount = 5;

namespace {
/*
 * Signal handler for SIGTERM. Sets the stop token to request a stop.
 * @param signal The signal number.
 */
auto stop_scheduler_handler(int signal) -> void {
    if (SIGTERM == signal) {
        spider::core::StopToken::get_instance().request_stop();
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
    desc.add_options()("no-exit", "Do not exit after receiving SIGTERM");

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
        spider::core::Scheduler const& scheduler,
        spider::core::StopToken& stop_token
) -> void {
    int fail_count = 0;
    while (!stop_token.stop_requested()) {
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
            stop_token.request_stop();
            break;
        }
    }
}

auto cleanup_loop(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::DataStorage> const& data_store,
        spider::core::StopToken const& stop_token
) -> void {
    while (!stop_token.stop_requested()) {
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
}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
auto main(int argc, char** argv) -> int {
    // Set up spdlog to write to stderr
    // NOLINTNEXTLINE(misc-include-cleaner)
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [spider.scheduler] %v");
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#endif

    boost::program_options::variables_map const args = parse_args(argc, argv);

    unsigned short port = 0;
    std::string scheduler_addr;
    std::string storage_url;
    bool no_exit = false;
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
        if (!args.contains("storage_url")) {
            spdlog::error("storage_url is required");
            return cCmdArgParseErr;
        }
        storage_url = args["storage_url"].as<std::string>();
        if (args.contains("no-exit")) {
            no_exit = true;
        }
    } catch (boost::bad_any_cast& e) {
        return cCmdArgParseErr;
    } catch (boost::program_options::error& e) {
        return cCmdArgParseErr;
    }

    // If not-exit is set, install signal handler for SIGTERM
    if (no_exit) {
        if (SIG_ERR == std::signal(SIGTERM, stop_scheduler_handler)) {
            spdlog::error("Failed to install signal handler for SIGTERM");
            return cSignalHandleErr;
        }
    }

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
    spider::core::StopToken stop_token;
    std::shared_ptr<spider::scheduler::SchedulerPolicy> const policy
            = std::make_shared<spider::scheduler::FifoPolicy>(
                    scheduler_id,
                    metadata_store,
                    data_store,
                    conn
            );
    spider::scheduler::SchedulerServer
            server{port, policy, metadata_store, data_store, conn, stop_token};

    try {
        // Start a thread that periodically updates the scheduler's heartbeat
        std::thread heartbeat_thread{
                heartbeat_loop,
                std::cref(storage_factory),
                std::cref(metadata_store),
                std::ref(scheduler),
                std::ref(spider::core::StopToken::get_instance()),
        };

        // Start a thread that periodically starts cleanup
        std::thread cleanup_thread{
                cleanup_loop,
                std::cref(storage_factory),
                std::cref(data_store),
                std::ref(spider::core::StopToken::get_instance())
        };

        heartbeat_thread.join();
        cleanup_thread.join();
        server.stop();
    } catch (std::system_error& e) {
        spdlog::error("Failed to join thread: {}", e.what());
    }

    if (no_exit) {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    return 0;
}
