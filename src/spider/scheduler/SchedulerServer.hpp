#ifndef SPIDER_SCHEDULER_SCHEDULERSERVER_HPP
#define SPIDER_SCHEDULER_SCHEDULERSERVER_HPP

#include "../io/BoostAsio.hpp"  // IWYU pragma: keep

namespace spider::scheduler {

class SchedulerServer {
public:
    // Delete copy constructor and copy assign operator
    SchedulerServer(SchedulerServer const&) = delete;
    auto operator=(SchedulerServer const&) -> SchedulerServer& = delete;

private:
    boost::asio::ip::tcp::acceptor m_acceptor;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERSERVER_HPP
