#include "FifoPolicy.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>

#include "../core/Data.hpp"
#include "../core/JobMetadata.hpp"
#include "../core/Task.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "SchedulerTaskCache.hpp"

namespace {

auto task_locality_satisfied(
        std::shared_ptr<spider::core::DataStorage> const& data_store,
        spider::core::StorageConnection& conn,
        spider::core::Task const& task,
        std::string const& addr
) -> bool {
    for (auto const& input : task.get_inputs()) {
        if (input.get_value().has_value()) {
            continue;
        }
        std::optional<boost::uuids::uuid> optional_data_id = input.get_data_id();
        if (!optional_data_id.has_value()) {
            continue;
        }
        boost::uuids::uuid const data_id = optional_data_id.value();
        spider::core::Data data;
        if (false == data_store->get_data(conn, data_id, &data).success()) {
            throw std::runtime_error(
                    fmt::format("Data with id {} not exists.", boost::uuids::to_string((data_id)))
            );
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

}  // namespace

namespace spider::scheduler {

FifoPolicy::FifoPolicy(
        std::shared_ptr<core::MetadataStorage> const& metadata_store,
        std::shared_ptr<core::DataStorage> const& data_store,
        core::StorageConnection& conn
)
        : m_metadata_store{metadata_store},
          m_data_store{data_store},
          m_conn{conn},
          m_task_cache{
                  metadata_store,
                  data_store,
                  conn,
                  [&](std::vector<core::Task>& tasks,
                      boost::uuids::uuid const& worker_id,
                      std::string const& worker_addr) -> std::optional<boost::uuids::uuid> {
                      return get_next_task(tasks, worker_id, worker_addr);
                  }
          } {}

auto FifoPolicy::get_next_task(
        std::vector<core::Task>& tasks,
        boost::uuids::uuid const& /*worker_id*/,
        std::string const& worker_addr
) -> std::optional<boost::uuids::uuid> {
    std::erase_if(tasks, [this, worker_addr](core::Task const& task) -> bool {
        return !task_locality_satisfied(m_data_store, m_conn, task, worker_addr);
    });

    if (tasks.empty()) {
        return std::nullopt;
    }

    auto const earliest_task = std::ranges::min_element(
            tasks,
            {},
            [this](core::Task const& task) -> std::chrono::system_clock::time_point {
                boost::uuids::uuid const task_id = task.get_id();
                boost::uuids::uuid job_id;
                std::optional<boost::uuids::uuid> const optional_job_id
                        = m_task_job_cache.get(task_id);
                if (optional_job_id.has_value()) {
                    job_id = optional_job_id.value();
                } else {
                    if (false
                        == m_metadata_store->get_task_job_id(m_conn, task_id, &job_id).success()) {
                        throw std::runtime_error(fmt::format(
                                "Task with id {} not exists.",
                                boost::uuids::to_string(task_id)
                        ));
                    }
                    m_task_job_cache.put(task_id, job_id);
                }

                std::optional<std::chrono::system_clock::time_point> const optional_time
                        = m_job_time_cache.get(job_id);
                if (optional_time.has_value()) {
                    return optional_time.value();
                }

                core::JobMetadata job_metadata;
                if (false
                    == m_metadata_store->get_job_metadata(m_conn, job_id, &job_metadata).success())
                {
                    throw std::runtime_error(fmt::format(
                            "Job with id {} not exists.",
                            boost::uuids::to_string(job_id)
                    ));
                }
                m_job_time_cache.put(job_id, job_metadata.get_creation_time());
                return job_metadata.get_creation_time();
            }
    );

    return earliest_task->get_id();
}

auto FifoPolicy::schedule_next(boost::uuids::uuid const worker_id, std::string const& worker_addr)
        -> std::optional<boost::uuids::uuid> {
    return m_task_cache.get_ready_task(worker_id, worker_addr);
}

}  // namespace spider::scheduler
