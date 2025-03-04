#include "FifoPolicy.hpp"

#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>

#include "../core/Data.hpp"
#include "../core/JobMetadata.hpp"
#include "../core/Task.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"

namespace spider::scheduler {

auto FifoPolicy::task_locality_satisfied(spider::core::Task const& task, std::string const& addr)
        -> bool {
    for (auto const& input : task.get_inputs()) {
        if (input.get_value().has_value()) {
            continue;
        }
        std::optional<boost::uuids::uuid> optional_data_id = input.get_data_id();
        if (!optional_data_id.has_value()) {
            continue;
        }
        boost::uuids::uuid const data_id = optional_data_id.value();
        core::Data data;
        if (m_data_cache.contains(data_id)) {
            data = m_data_cache[data_id];
        } else {
            if (false == m_data_store->get_data(m_conn, data_id, &data).success()) {
                throw std::runtime_error(
                        fmt::format("Data with id {} not exists.", to_string((data_id)))
                );
            }
            m_data_cache.emplace(data_id, data);
        }
        if (false == data.is_hard_locality()) {
            continue;
        }
        std::vector<std::string> const& locality = data.get_locality();
        if (locality.empty()) {
            continue;
        }
        if (std::ranges::find(locality, addr) == locality.end()) {
            return false;
        }
    }
    return true;
}

FifoPolicy::FifoPolicy(
        std::shared_ptr<core::MetadataStorage> const& metadata_store,
        std::shared_ptr<core::DataStorage> const& data_store,
        core::StorageConnection& conn
)
        : m_metadata_store{metadata_store},
          m_data_store{data_store},
          m_conn{conn} {}

auto FifoPolicy::schedule_next(
        boost::uuids::uuid const /*worker_id*/,
        std::string const& worker_addr
) -> std::optional<boost::uuids::uuid> {
    if (m_tasks.empty()) {
        fetch_tasks();
        if (m_tasks.empty()) {
            return std::nullopt;
        }
    }
    auto const reverse_begin = std::reverse_iterator(m_tasks.end());
    auto const reverse_end = std::reverse_iterator(m_tasks.begin());
    auto const it = std::find_if(reverse_begin, reverse_end, [&](core::Task const& task) {
        return task_locality_satisfied(task, worker_addr);
    });
    if (it == reverse_end) {
        return std::nullopt;
    }
    m_tasks.erase(it.base());
    for (core::TaskInput const& input : it->get_inputs()) {
        std::optional<boost::uuids::uuid> const data_id = input.get_data_id();
        if (data_id.has_value()) {
            m_data_cache.erase(data_id.value());
        }
    }
    return it->get_id();
}

auto FifoPolicy::fetch_tasks() -> void {
    m_data_cache.clear();
    m_metadata_store->get_ready_tasks(m_conn, &m_tasks);
    std::vector<std::tuple<core::TaskInstance, core::Task>> instances;
    m_metadata_store->get_task_timeout(m_conn, &instances);
    for (auto const& [instance, task] : instances) {
        m_tasks.emplace_back(task);
    }

    // Sort tasks based on job creation time in descending order.
    // NOLINTNEXTLINE(misc-include-cleaner)
    absl::flat_hash_map<boost::uuids::uuid, core::JobMetadata, std::hash<boost::uuids::uuid>>
            job_metadata_map;
    auto get_task_job_creation_time
            = [&](boost::uuids::uuid const task_id) -> std::chrono::system_clock::time_point {
        boost::uuids::uuid job_id;
        if (false == m_metadata_store->get_task_job_id(m_conn, task_id, &job_id).success()) {
            throw std::runtime_error(fmt::format("Task with id {} not exists.", to_string(task_id))
            );
        }
        if (job_metadata_map.contains(job_id)) {
            return job_metadata_map[job_id].get_creation_time();
        }
        core::JobMetadata job_metadata;
        if (false == m_metadata_store->get_job_metadata(m_conn, job_id, &job_metadata).success()) {
            throw std::runtime_error(fmt::format("Job with id {} not exists.", to_string(job_id)));
        }
        job_metadata_map[job_id] = job_metadata;
        return job_metadata.get_creation_time();
    };
    std::ranges::sort(m_tasks, [&](core::Task const& a, core::Task const& b) {
        return get_task_job_creation_time(a.get_id()) > get_task_job_creation_time(b.get_id());
    });
}

}  // namespace spider::scheduler
