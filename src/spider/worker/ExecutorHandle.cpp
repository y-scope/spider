#include "ExecutorHandle.hpp"

#include <mutex>
#include <optional>

#include <boost/uuid/uuid.hpp>

#include "TaskExecutor.hpp"

namespace spider::worker {
auto ExecutorHandle::get_task_id() -> std::optional<boost::uuids::uuid> {
    std::lock_guard const lock_guard{m_mutex};
    if (nullptr != m_executor) {
        return m_task_id;
    }
    return std::nullopt;
}

auto ExecutorHandle::get_executor() -> TaskExecutor* {
    std::lock_guard const lock_guard{m_mutex};
    return m_executor;
}

auto ExecutorHandle::set(boost::uuids::uuid const task_id, TaskExecutor* executor) -> void {
    std::lock_guard const lock_guard{m_mutex};
    m_task_id = task_id;
    m_executor = executor;
}

auto ExecutorHandle::clear() -> void {
    std::lock_guard const lock_guard{m_mutex};
    m_task_id = boost::uuids::uuid{};
    m_executor = nullptr;
}
}  // namespace spider::worker
