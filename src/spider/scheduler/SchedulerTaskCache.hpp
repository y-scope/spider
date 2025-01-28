#ifndef SPIDER_SCHEDULER_SCHEDULERTASKCACHE_HPP
#define SPIDER_SCHEDULER_SCHEDULERTASKCACHE_HPP

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/uuid/uuid.hpp>

#include "../core/Task.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"

namespace spider::scheduler {

class SchedulerTaskCache {
public:
    SchedulerTaskCache(
            std::shared_ptr<core::MetadataStorage> const& metadata_store,
            std::shared_ptr<core::DataStorage> const& data_store,
            std::function<std::optional<boost::uuids::uuid>(
                    std::vector<core::Task>& tasks,
                    boost::uuids::uuid const& worker_id,
                    std::string const& worker_addr
            )> const& get_next_task_function
    )
            : m_metadata_store{metadata_store},
              m_data_store{data_store},
              m_get_next_task_function{get_next_task_function} {}

    auto get_ready_task(boost::uuids::uuid const& worker_id, std::string const& worker_addr)
            -> std::optional<boost::uuids::uuid>;

private:
    auto should_fetch_tasks() -> bool;

    void fetch_ready_tasks();

    auto pop_next_task(boost::uuids::uuid const& worker_id, std::string const& worker_addr)
            -> std::optional<core::Task>;

    std::shared_ptr<core::MetadataStorage> m_metadata_store;
    std::shared_ptr<core::DataStorage> m_data_store;

    // NOLINTNEXTLINE(misc-include-cleaner)
    absl::flat_hash_map<boost::uuids::uuid, core::Task, std::hash<boost::uuids::uuid>> m_tasks;
    std::chrono::steady_clock::time_point m_last_update;
    size_t m_update_count = 0;

    std::function<std::optional<boost::uuids::uuid>(
            std::vector<core::Task>& tasks,
            boost::uuids::uuid const& worker_id,
            std::string const& worker_addr
    )>
            m_get_next_task_function;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERTASKCACHE_HPP
