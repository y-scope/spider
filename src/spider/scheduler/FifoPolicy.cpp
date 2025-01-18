#include "FifoPolicy.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>

#include "../core/Data.hpp"
#include "../core/JobMetadata.hpp"
#include "../core/Task.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"

namespace {

auto task_locality_satisfied(
        std::shared_ptr<spider::core::DataStorage> const& data_store,
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
        if (false == data_store->get_data(data_id, &data).success()) {
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

auto FifoPolicy::schedule_next(
        std::shared_ptr<core::MetadataStorage> metadata_store,
        std::shared_ptr<core::DataStorage> data_store,
        boost::uuids::uuid const /*worker_id*/,
        std::string const& worker_addr
) -> std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> {
    std::vector<core::Task> ready_tasks;
    metadata_store->get_ready_tasks(&ready_tasks);

    std::erase_if(ready_tasks, [data_store, worker_addr](core::Task const& task) -> bool {
        return !task_locality_satisfied(data_store, task, worker_addr);
    });

    if (ready_tasks.empty()) {
        return std::nullopt;
    }

    auto const earliest_task = std::ranges::min_element(
            ready_tasks,
            {},
            [this,
             metadata_store](core::Task const& task) -> std::chrono::system_clock::time_point {
                boost::uuids::uuid const task_id = task.get_id();
                boost::uuids::uuid job_id;
                std::optional<boost::uuids::uuid> const optional_job_id
                        = m_task_job_cache.get(task_id);
                if (optional_job_id.has_value()) {
                    job_id = optional_job_id.value();
                } else {
                    if (false == metadata_store->get_task_job_id(task_id, &job_id).success()) {
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
                if (false == metadata_store->get_job_metadata(job_id, &job_metadata).success()) {
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

auto FifoPolicy::cleanup() -> void {
    m_task_job_cache.cleanup();
    m_job_time_cache.cleanup();
}

}  // namespace spider::scheduler
