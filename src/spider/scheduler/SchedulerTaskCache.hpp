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

namespace spider::core {

class SchedulerTaskCache {
public:
    SchedulerTaskCache(
            std::shared_ptr<MetadataStorage> const& metadata_store,
            std::shared_ptr<DataStorage> const& data_store,
            size_t const num_tasks,
            std::function<std::optional<boost::uuids::uuid>(
                    std::vector<Task>& tasks,
                    boost::uuids::uuid const& worker_id,
                    std::string const& worker_addr
            )> const& get_next_task_function
    )
            : m_metadata_store{metadata_store},
              m_data_store{data_store},
              m_num_tasks{num_tasks},
              m_get_next_task_function{get_next_task_function} {}

    auto get_ready_task(boost::uuids::uuid const& worker_id, std::string const& worker_addr)
            -> std::optional<boost::uuids::uuid>;

private:
    auto should_fetch_tasks() -> bool;

    void fetch_ready_tasks();

    auto pop_next_task(boost::uuids::uuid const& worker_id, std::string const& worker_addr)
            -> std::optional<Task>;

    std::shared_ptr<MetadataStorage> m_metadata_store;
    std::shared_ptr<DataStorage> m_data_store;

    absl::flat_hash_map<boost::uuids::uuid, Task> m_tasks;
    std::chrono::steady_clock::time_point m_last_update;
    size_t m_update_count = 0;

    size_t m_num_tasks;

    std::function<std::optional<boost::uuids::uuid>(
            std::vector<Task>& tasks,
            boost::uuids::uuid const& worker_id,
            std::string const& worker_addr
    )>
            m_get_next_task_function;
};

}  // namespace spider::core

#endif  // SPIDER_SCHEDULER_SCHEDULERTASKCACHE_HPP
