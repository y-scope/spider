
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <thread>

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
#include "../storage/MySqlStorage.hpp"
#include "../utils/StopToken.hpp"
#include "FifoPolicy.hpp"
#include "SchedulerPolicy.hpp"
#include "SchedulerServer.hpp"

constexpr int cCmdArgParseErr = 1;
constexpr int cStorageConnectionErr = 2;
constexpr int cSchedulerAddrErr = 3;
constexpr int cStorageErr = 4;

constexpr int cCleanupInterval = 5;
constexpr int cRetryCount = 5;

namespace {
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
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        spider::core::Scheduler const& scheduler,
        spider::core::StopToken& stop_token
) -> void {
    int fail_count = 0;
    while (!stop_token.stop_requested()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        spdlog::debug("Updating heartbeat");
        spider::core::StorageErr const err = metadata_store->update_heartbeat(scheduler.get_id());
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
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        std::shared_ptr<spider::core::DataStorage> const& data_store,
        spider::scheduler::SchedulerServer& server,
        std::shared_ptr<spider::scheduler::SchedulerPolicy> const& policy,
        spider::core::Scheduler const& scheduler,
        spider::core::StopToken& stop_token
) -> void {
    while (!stop_token.stop_requested()) {
        std::this_thread::sleep_for(std::chrono::seconds(cCleanupInterval));
        spdlog::debug("Starting cleanup");
        spider::core::StorageErr err
                = metadata_store->set_scheduler_state(scheduler.get_id(), "gc");
        if (!err.success()) {
            spdlog::error("Failed to set scheduler state to gc: {}", err.description);
            continue;
        }
        data_store->remove_dangling_data();
        for (size_t i = 0; i < cRetryCount; ++i) {
            err = metadata_store->set_scheduler_state(scheduler.get_id(), "normal");
            if (!err.success()) {
                spdlog::error("Failed to set scheduler state to normal: {}", err.description);
                if (i >= cRetryCount - 1) {
                    stop_token.request_stop();
                    return;
                }
            } else {
                break;
            }
        }
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
    } catch (boost::bad_any_cast& e) {
        return cCmdArgParseErr;
    } catch (boost::program_options::error& e) {
        return cCmdArgParseErr;
    }

    // Create storages
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = std::make_shared<spider::core::MySqlMetadataStorage>(storage_url);
    std::shared_ptr<spider::core::DataStorage> const data_store
            = std::make_shared<spider::core::MySqlDataStorage>(storage_url);

    // Initialize storages
    spider::core::StorageErr err = metadata_store->initialize();
    if (!err.success()) {
        spdlog::error("Failed to initialize metadata storage: {}", err.description);
        return cStorageErr;
    }
    err = data_store->initialize();
    if (!err.success()) {
        spdlog::error("Failed to initialize data storage: {}", err.description);
        return cStorageErr;
    }

    // Get scheduler id and addr
    boost::uuids::random_generator gen;
    boost::uuids::uuid const scheduler_id = gen();

    // Start scheduler server
    spider::core::StopToken stop_token;
    std::shared_ptr<spider::scheduler::SchedulerPolicy> const policy
            = std::make_shared<spider::scheduler::FifoPolicy>(metadata_store, data_store);
    spider::scheduler::SchedulerServer server{port, policy, metadata_store, data_store, stop_token};

    // Register scheduler with storage
    spider::core::Scheduler const scheduler{scheduler_id, scheduler_addr, port};
    err = metadata_store->add_scheduler(scheduler);
    if (!err.success()) {
        spdlog::error("Failed to register scheduler with storage server: {}", err.description);
        return cStorageErr;
    }

    try {
        // Start a thread that periodically updates the scheduler's heartbeat
        std::thread heartbeat_thread{
                heartbeat_loop,
                std::cref(metadata_store),
                std::ref(scheduler),
                std::ref(stop_token),
        };

        // Start a thread that periodically starts cleanup
        std::thread cleanup_thread{
                cleanup_loop,
                std::cref(metadata_store),
                std::cref(data_store),
                std::ref(server),
                std::cref(policy),
                std::cref(scheduler),
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
