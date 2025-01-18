#ifndef SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP
#define SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP

#include <memory>
#include <optional>
#include <string>
#include <tuple>

#include <boost/uuid/uuid.hpp>

#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"

namespace spider::scheduler {
class SchedulerPolicy {
public:
    SchedulerPolicy() = default;
    SchedulerPolicy(SchedulerPolicy const&) = default;
    auto operator=(SchedulerPolicy const&) -> SchedulerPolicy& = default;
    SchedulerPolicy(SchedulerPolicy&&) = default;
    auto operator=(SchedulerPolicy&&) -> SchedulerPolicy& = default;
    virtual ~SchedulerPolicy() = default;

    virtual auto schedule_next(
            std::shared_ptr<core::MetadataStorage> metadata_store,
            std::shared_ptr<core::DataStorage> data_store,
            boost::uuids::uuid worker_id,
            std::string const& worker_addr
    ) -> std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> = 0;

    virtual auto cleanup() -> void = 0;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP
