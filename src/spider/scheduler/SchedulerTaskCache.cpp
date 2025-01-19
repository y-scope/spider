#include "SchedulerTaskCache.hpp"

#include <chrono>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Task.hpp"

namespace spider::scheduler {

namespace {
constexpr int cUpdateCount = 100;
constexpr int cUpdateInterval = 5;  // 10 ms
}  // namespace

auto SchedulerTaskCache::get_ready_task(
        boost::uuids::uuid const& worker_id,
        std::string const& worker_addr
) -> std::optional<boost::uuids::uuid> {
    bool const updated = should_fetch_tasks();
    if (updated) {
        fetch_ready_tasks();
    }

    std::optional<core::Task> task = pop_next_task(worker_id, worker_addr);
    if (task.has_value()) {
        m_update_count++;
        return task.value().get_id();
    }
    if (updated) {
        m_update_count++;
        return std::nullopt;
    }
    fetch_ready_tasks();
    task = pop_next_task(worker_id, worker_addr);
    m_update_count++;
    if (task.has_value()) {
        return task.value().get_id();
    }

    return std::nullopt;
}

auto SchedulerTaskCache::pop_next_task(
        boost::uuids::uuid const& worker_id,
        std::string const& worker_addr
) -> std::optional<core::Task> {
    std::vector<core::Task> tasks;
    tasks.reserve(m_tasks.size());
    for (auto const& task : m_tasks) {
        tasks.push_back(task.second);
    }
    std::optional<boost::uuids::uuid> const task_id = std::invoke(
            m_get_next_task_function,
            std::ref(tasks),
            std::cref(worker_id),
            std::cref(worker_addr)
    );
    if (!task_id.has_value()) {
        return std::nullopt;
    }

    core::Task task = m_tasks.at(task_id.value());
    m_tasks.erase(task_id.value());
    return task;
}

auto SchedulerTaskCache::should_fetch_tasks() -> bool {
    std::chrono::steady_clock::time_point const now = std::chrono::steady_clock::now();
    if (m_last_update + std::chrono::milliseconds(cUpdateInterval) < now) {
        return true;
    }
    return m_update_count > cUpdateCount;
}

void SchedulerTaskCache::fetch_ready_tasks() {
    m_tasks.clear();
    std::vector<core::Task> tasks;
    m_metadata_store->get_ready_tasks(&tasks);
    for (core::Task const& task : tasks) {
        m_tasks.emplace(std::make_pair(task.get_id(), task));
    }

    m_update_count = 0;
    m_last_update = std::chrono::steady_clock::now();
}

}  // namespace spider::scheduler
