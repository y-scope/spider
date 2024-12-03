#ifndef SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP
#define SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP

#include <memory>

#include <boost/uuid/uuid.hpp>

#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"

namespace spider::scheduler {
class SchedulerPolicy {
public:
    virtual ~SchedulerPolicy() = default;

    virtual auto schedule_next(
            std::shared_ptr<core::MetadataStorage> metadata_store,
            std::shared_ptr<core::DataStorage> data_store
    ) -> boost::uuids::uuid = 0;

    virtual auto cleanup_job(boost::uuids::uuid job_id) -> void = 0;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERPOLICY_HPP
