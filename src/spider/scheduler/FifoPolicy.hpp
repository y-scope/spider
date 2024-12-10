#ifndef SPIDER_SCHEDULER_FIFOPOLICY_HPP
#define SPIDER_SCHEDULER_FIFOPOLICY_HPP

#include <chrono>
#include <memory>
#include <optional>
#include <string>

#include <boost/uuid/uuid.hpp>

#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../utils/TimedCache.hpp"
#include "SchedulerPolicy.hpp"

namespace spider::scheduler {

class FifoPolicy final : public SchedulerPolicy {
public:
    auto schedule_next(
            std::shared_ptr<core::MetadataStorage> metadata_store,
            std::shared_ptr<core::DataStorage> data_store,
            boost::uuids::uuid worker_id,
            std::string const& worker_addr
    ) -> std::optional<boost::uuids::uuid> override;
    auto cleanup() -> void override;

private:
    core::TimedCache<boost::uuids::uuid, boost::uuids::uuid> m_task_job_cache;
    core::TimedCache<boost::uuids::uuid, std::chrono::system_clock::time_point> m_job_time_cache;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_FIFOPOLICY_HPP
