#ifndef SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP
#define SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP

#include <optional>
#include <string>

#include <boost/uuid/uuid.hpp>

namespace spider::scheduler {
class SchedulerPolicy {
public:
    SchedulerPolicy() = default;
    SchedulerPolicy(SchedulerPolicy const&) = default;
    auto operator=(SchedulerPolicy const&) -> SchedulerPolicy& = default;
    SchedulerPolicy(SchedulerPolicy&&) = default;
    auto operator=(SchedulerPolicy&&) -> SchedulerPolicy& = default;
    virtual ~SchedulerPolicy() = default;

    virtual auto schedule_next(boost::uuids::uuid worker_id, std::string const& worker_addr)
            -> std::optional<boost::uuids::uuid>
            = 0;
};
}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP
