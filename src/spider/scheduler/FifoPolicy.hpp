#ifndef SPIDER_SCHEDULER_FIFOPOLICY_HPP
#define SPIDER_SCHEDULER_FIFOPOLICY_HPP

#include <chrono>

#include <absl/container/flat_hash_map.h>

#include "SchedulerPolicy.hpp"

namespace spider::scheduler {

class FifoPolicy final : public SchedulerPolicy {
public:
    auto schedule_next(
            std::shared_ptr<core::MetadataStorage> metadata_store,
            std::shared_ptr<core::DataStorage> data_store
    ) -> boost::uuids::uuid override;
    auto cleanup_job(boost::uuids::uuid job_id) -> void override;

private:
    absl::flat_hash_map<boost::uuids::uuid, std::chrono::system_clock::time_point> m_job_time_map;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_FIFOPOLICY_HPP
