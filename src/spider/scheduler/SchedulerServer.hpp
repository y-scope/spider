#ifndef SPIDER_SCHEDULER_SCHEDULERSERVER_HPP
#define SPIDER_SCHEDULER_SCHEDULERSERVER_HPP

#include <memory>
#include <mutex>

#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "SchedulerPolicy.hpp"

namespace spider::scheduler {

class SchedulerServer {
public:
    // Delete copy & move constructor and assignment operator
    SchedulerServer(SchedulerServer const&) = delete;
    auto operator=(SchedulerServer const&) -> SchedulerServer& = delete;
    SchedulerServer(SchedulerServer&&) = delete;
    auto operator=(SchedulerServer&&) noexcept -> SchedulerServer& = delete;
    ~SchedulerServer() = default;

    SchedulerServer(
            unsigned short port,
            std::shared_ptr<SchedulerPolicy> policy,
            std::shared_ptr<core::MetadataStorage> metadata_store,
            std::shared_ptr<core::DataStorage> data_store
    );

    /**
     * Run the server loop. This function blocks until stop is called.
     */
    auto run() -> void;

    auto stop() -> void;

private:
    auto receive_message() -> boost::asio::awaitable<void>;

    auto process_message(boost::asio::ip::tcp::socket socket) -> boost::asio::awaitable<void>;

    auto should_stop() -> bool;

    std::shared_ptr<SchedulerPolicy> m_policy;
    std::shared_ptr<core::MetadataStorage> m_metadata_store;
    std::shared_ptr<core::DataStorage> m_data_store;

    boost::asio::io_context m_context;
    boost::asio::ip::tcp::acceptor m_acceptor;

    std::mutex m_mutex;
    bool m_stop = false;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERSERVER_HPP
