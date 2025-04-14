#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>

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
#include "../utils/ProgramOptions.hpp"
#include "../utils/StopToken.hpp"
#include "FifoPolicy.hpp"
#include "SchedulerPolicy.hpp"
#include "SchedulerServer.hpp"

constexpr int cCmdArgParseErr = 1;
constexpr int cStorageConnectionErr = 2;
constexpr int cSchedulerAddrErr = 3;
constexpr int cStorageErr = 4;

constexpr int cCleanupInterval = 1000;
constexpr int cRetryCount = 5;

namespace {
auto parse_args(
        int const argc,
        char** argv,
        std::string& host,
        unsigned short& port,
        std::string& storage_url
) -> bool {
    boost::program_options::options_description desc;
    // clang-format off
    desc.add_options()
        (spider::core::cHelpOption.data(), spider::core::cHelpMessage.data())
        (
            spider::core::cHostOption.data(),
            boost::program_options::value<std::string>(&host)->required(),
            spider::core::cHostMessage.data()
        )
        (
            spider::core::cPortOption.data(),
            boost::program_options::value<unsigned short>(&port)->required(),
            spider::core::cPortMessage.data()
        )
        (
            spider::core::cStorageUrlOption.data(),
            boost::program_options::value<std::string>(&storage_url)->required(),
            spider::core::cStorageUrlMessage.data()
        );
    // clang-format on

    try {
        boost::program_options::variables_map variables;
        boost::program_options::store(
                // NOLINTNEXTLINE(misc-include-cleaner)
                boost::program_options::parse_command_line(argc, argv, desc),
                variables
        );

        if (false == variables.contains(std::string(spider::core::cHostOption))
            && false == variables.contains(std::string(spider::core::cPortOption))
            && false == variables.contains(std::string(spider::core::cStorageUrlOption)))
        {
            std::cout << spider::core::cSchedulerUsage << "\n";
            std::cout << desc << "\n";
            return false;
        }

        boost::program_options::notify(variables);

        if (host.empty()) {
            std::cerr << spider::core::cHostEmptyMessage << "\n";
            return false;
        }

        if (storage_url.empty()) {
            std::cerr << spider::core::cStorageUrlEmptyMessage << "\n";
            return false;
        }

        return true;
    } catch (boost::program_options::error& e) {
        std::cerr << "spider_scheduler: " << e.what() << "\n";
        std::cerr << spider::core::cSchedulerUsage << "\n";
        std::cerr << spider::core::cSchedulerHelpMessage;
        return false;
    }
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

    unsigned short port = 0;
    std::string scheduler_addr;
    std::string storage_url;
    if (false == parse_args(argc, argv, scheduler_addr, port, storage_url)) {
        return cCmdArgParseErr;
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
                std::ref(stop_token),
        };

        // Start a thread that periodically starts cleanup
        std::thread cleanup_thread{
                cleanup_loop,
                std::cref(storage_factory),
                std::cref(data_store),
                std::ref(stop_token)
        };

        heartbeat_thread.join();
        cleanup_thread.join();
        server.stop();
    } catch (std::system_error& e) {
        spdlog::error("Failed to join thread: {}", e.what());
    }

    return 0;
}
