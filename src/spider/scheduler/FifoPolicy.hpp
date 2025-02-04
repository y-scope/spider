#ifndef SPIDER_SCHEDULER_FIFOPOLICY_HPP
#define SPIDER_SCHEDULER_FIFOPOLICY_HPP

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Task.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../utils/TimedCache.hpp"
#include "SchedulerPolicy.hpp"
#include "SchedulerTaskCache.hpp"

namespace spider::scheduler {

class FifoPolicy final : public SchedulerPolicy {
public:
    FifoPolicy(
            std::shared_ptr<core::MetadataStorage> const& metadata_store,
            std::shared_ptr<core::DataStorage> const& data_store
    );

    auto schedule_next(boost::uuids::uuid worker_id, std::string const& worker_addr)
            -> std::optional<boost::uuids::uuid> override;
    auto cleanup() -> void override;

private:
    auto get_next_task(
            std::vector<core::Task>& tasks,
            boost::uuids::uuid const& worker_id,
            std::string const& worker_addr
    ) -> std::optional<boost::uuids::uuid>;

    std::shared_ptr<core::MetadataStorage> m_metadata_store;
    std::shared_ptr<core::DataStorage> m_data_store;

    SchedulerTaskCache m_task_cache;

    core::TimedCache<boost::uuids::uuid, boost::uuids::uuid> m_task_job_cache;
    core::TimedCache<boost::uuids::uuid, std::chrono::system_clock::time_point> m_job_time_cache;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_FIFOPOLICY_HPP
