#include "JobRecovery.hpp"

#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <tuple>
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

    for (auto const& [task_id, task] : m_task_graph.get_tasks()) {
        if (TaskState::Failed == task.get_state()) {
            m_task_set.insert(task_id);
            m_task_queue.push_front(task_id);
        }
    }

    while (!m_task_queue.empty()) {
        auto const task_id = m_task_queue.front();
        m_task_queue.pop_front();
        err = process_task(task_id);
        if (false == err.success()) {
            return err;
        }
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

auto JobRecovery::check_task_input(
        Task const& task,
        absl::flat_hash_set<boost::uuids::uuid>& not_persisted
) -> StorageErr {
    for (auto const& task_input : task.get_inputs()) {
        std::optional<boost::uuids::uuid> optional_data_id = task_input.get_data_id();
        if (false == optional_data_id.has_value()) {
            continue;
        }
        boost::uuids::uuid const data_id = optional_data_id.value();
        Data data;
        StorageErr err = get_data(data_id, data);
        if (false == err.success()) {
            return err;
        }
        if (false == data.is_persisted()) {
            std::optional<std::tuple<boost::uuids::uuid, uint8_t>> optional_parent
                    = task_input.get_task_output();
            if (false == optional_parent.has_value()) {
                continue;
            }
            boost::uuids::uuid const parent_task_id = std::get<0>(optional_parent.value());
            not_persisted.insert(parent_task_id);
        }
    }
    return StorageErr{};
}

auto JobRecovery::process_task(boost::uuids::uuid task_id) -> StorageErr {
    std::optional<Task*> const optional_task = m_task_graph.get_task(task_id);
    if (false == optional_task.has_value()) {
        return StorageErr{
                StorageErrType::KeyNotFoundErr,
                fmt::format("No task with id {}", to_string(task_id))
        };
    }

    for (boost::uuids::uuid const& child_id : m_task_graph.get_child_tasks(task_id)) {
        if (m_task_set.contains(child_id)) {
            continue;
        }
        std::optional<Task*> optional_child_task = m_task_graph.get_task(child_id);
        if (false == optional_child_task.has_value()) {
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("No task with id {}", to_string(child_id))
            };
        }
        Task const& child_task = *optional_child_task.value();
        if (TaskState::Pending != child_task.get_state()) {
            m_task_queue.push_back(child_id);
            m_task_set.insert(child_id);
        }
    }

    Task const& task = *optional_task.value();
    absl::flat_hash_set<boost::uuids::uuid> not_persisted;
    StorageErr err = check_task_input(task, not_persisted);
    if (false == err.success()) {
        return err;
    }

    if (not_persisted.empty()) {
        m_ready_tasks.insert(task_id);
    } else {
        m_pending_tasks.insert(task_id);
        for (auto const& parent_id : not_persisted) {
            if (false == m_task_set.contains(parent_id)) {
                m_task_queue.push_back(parent_id);
                m_task_set.insert(parent_id);
            }
        }
    }

    return StorageErr{};
}

auto JobRecovery::get_pending_tasks() -> std::vector<boost::uuids::uuid> {
    std::vector<boost::uuids::uuid> pending_tasks;
    pending_tasks.reserve(m_pending_tasks.size());
    for (auto const& task_id : m_pending_tasks) {
        pending_tasks.push_back(task_id);
    }
    return pending_tasks;
}

auto JobRecovery::get_ready_tasks() -> std::vector<boost::uuids::uuid> {
    std::vector<boost::uuids::uuid> ready_tasks;
    ready_tasks.reserve(m_ready_tasks.size());
    for (auto const& task_id : m_ready_tasks) {
        ready_tasks.push_back(task_id);
    }
    return ready_tasks;
}
}  // namespace spider::core
