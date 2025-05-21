#include "JobRecovery.hpp"

#include <deque>
#include <memory>
#include <utility>

#include <absl/container/flat_hash_set.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>

#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "absl/container/flat_hash_set.h"

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
    StorageErr const err = m_metadata_store->get_task_graph(*m_conn, m_job_id, &m_task_graph);
    if (false == err.success()) {
        return err;
    }

    // Get all the failed tasks
    absl::flat_hash_set<boost::uuids::uuid> task_set;
    for (auto const& pair : m_task_graph.get_tasks()) {
        Task const& task = pair.second;
        if (TaskState::Failed == task.get_state()) {
            task_set.insert(pair.first);
        }
    }

    absl::flat_hash_set<boost::uuids::uuid> ready_task_set;
    absl::flat_hash_set<boost::uuids::uuid> pending_task_set;
    // For each task pop from the set, check if its inputs contains non-persisted Data.
    // If so, add it to the pending task set and add parent in the task_set. Otherwise, add it to
    // the ready task set.
    std::deque<boost::uuids::uuid> working_set;
    for (auto const& task_id : task_set) {
        working_set.push_back(task_id);
    }
    while (!working_set.empty()) {
        auto const task_id = working_set.front();
        working_set.pop_front();
        std::optional<Task*> optional_task = m_task_graph.get_task(task_id);
        if (false == optional_task.has_value()) {
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("No task with id {}", to_string(task_id))
            };
        }
    }
}

auto JobRecovery::check_task_input(Task const& task, bool& not_persisted) -> StorageErr {
    for (auto const& task_input : task.get_inputs()) {
        std::optional<boost::uuids::uuid> optional_date_id = task_input.get_data_id();
        if (false == optional_date_id.has_value()) {
            continue;
        }
        boost::uuids::uuid const data_id = optional_date_id.value();
        Data data;
        StorageErr const err = m_data_store->get_data(*m_conn, data_id, &data);
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

auto JobRecovery::get_pending_tasks() -> std::vector<boost::uuids::uuid> {
    return {};
}

auto JobRecovery::get_ready_tasks() -> std::vector<boost::uuids::uuid> {
    return {};
}
}  // namespace spider::core
