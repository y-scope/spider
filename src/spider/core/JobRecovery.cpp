#include "JobRecovery.hpp"

#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_set.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>

#include <spider/core/Data.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/Task.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>

namespace spider::core {
JobRecovery::JobRecovery(
        boost::uuids::uuid const job_id,
        std::shared_ptr<StorageConnection> storage_connection,
        std::shared_ptr<DataStorage> data_store,
        std::shared_ptr<MetadataStorage> metadata_store
)
        : m_job_id{job_id},
          m_conn{std::move(storage_connection)},
          m_data_store{std::move(data_store)},
          m_metadata_store{std::move(metadata_store)} {}

auto JobRecovery::compute_graph() -> StorageErr {
    StorageErr err = m_metadata_store->get_task_graph(*m_conn, m_job_id, &m_task_graph);
    if (false == err.success()) {
        return err;
    }

    // Get all the failed tasks
    absl::flat_hash_set<boost::uuids::uuid> task_set;
    for (auto const& [task_id, task] : m_task_graph.get_tasks()) {
        if (TaskState::Failed == task.get_state()) {
            task_set.insert(task_id);
        }
    }

    absl::flat_hash_set<boost::uuids::uuid> ready_task_set;
    absl::flat_hash_set<boost::uuids::uuid> pending_task_set;
    // For each task pop from the set, check if its inputs contains non-persisted Data.
    // If the task has non-persisted Data input and has parents, add it to pending tasks and add
    // its parents to the working queue. If the task has non-persisted Data input and has no
    // parents, or the task has all its inputs persisted, add it to ready tasks.
    std::deque<boost::uuids::uuid> working_queue;
    for (auto const& task_id : task_set) {
        working_queue.push_back(task_id);
    }
    while (!working_queue.empty()) {
        auto const task_id = working_queue.front();
        working_queue.pop_front();
        std::optional<Task*> optional_task = m_task_graph.get_task(task_id);
        if (false == optional_task.has_value()) {
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("No task with id {}", to_string(task_id))
            };
        }
        Task const& task = *optional_task.value();
        bool not_persisted = false;
        err = check_task_input(task, not_persisted);
        if (false == err.success()) {
            return err;
        }
        if (not_persisted) {
            std::vector<boost::uuids::uuid> const parents = m_task_graph.get_parent_tasks(task_id);
            if (parents.empty()) {
                ready_task_set.insert(task_id);
            } else {
                pending_task_set.insert(task_id);
                for (auto const& parent_id : parents) {
                    if (false == task_set.contains(parent_id)) {
                        working_queue.push_back(parent_id);
                        task_set.insert(parent_id);
                    }
                }
            }
        } else {
            ready_task_set.insert(task_id);
        }
    }

    // Set the pending and ready tasks
    m_pending_tasks.clear();
    m_pending_tasks.reserve(pending_task_set.size());
    for (auto const& task_id : pending_task_set) {
        m_pending_tasks.push_back(task_id);
    }
    m_ready_tasks.clear();
    m_ready_tasks.reserve(ready_task_set.size());
    for (auto const& task_id : ready_task_set) {
        m_ready_tasks.push_back(task_id);
    }

    return StorageErr{};
}

auto JobRecovery::get_data(boost::uuids::uuid data_id, Data& data) -> StorageErr {
    auto it = m_data_map.find(data_id);
    if (it != m_data_map.end()) {
        data = it->second;
        return StorageErr{};
    }
    StorageErr const err = m_data_store->get_data(*m_conn, data_id, &data);
    if (err.success()) {
        m_data_map[data_id] = data;
    }
    return err;
}

auto JobRecovery::check_task_input(Task const& task, bool& not_persisted) -> StorageErr {
    for (auto const& task_input : task.get_inputs()) {
        std::optional<boost::uuids::uuid> optional_date_id = task_input.get_data_id();
        if (false == optional_date_id.has_value()) {
            continue;
        }
        boost::uuids::uuid const data_id = optional_date_id.value();
        Data data;
        StorageErr err = get_data(data_id, data);
        if (false == err.success()) {
            return err;
        }
        if (data.is_persisted()) {
            continue;
        }
        not_persisted = true;
        return StorageErr{};
    }
    not_persisted = false;
    return StorageErr{};
}

auto JobRecovery::get_pending_tasks() -> std::vector<boost::uuids::uuid> const& {
    return m_pending_tasks;
}

auto JobRecovery::get_ready_tasks() -> std::vector<boost::uuids::uuid> const& {
    return m_ready_tasks;
}
}  // namespace spider::core
