#include "ExecutorHandle.hpp"

#include <mutex>
#include <optional>

#include <boost/uuid/uuid.hpp>

#include "TaskExecutor.hpp"

namespace spider::worker {
auto ExecutorHandle::get_task_id() -> std::optional<boost::uuids::uuid> {
    std::lock_guard const lock_guard{m_mutex};
    if (nullptr != m_executor) {
        return m_executor->get_task_id();
    }
    return std::nullopt;
}

auto ExecutorHandle::cancel_executor() -> void {
    std::lock_guard const lock_guard{m_mutex};
    if (nullptr != m_executor) {
        m_executor->cancel();
    }
}

auto ExecutorHandle::set(TaskExecutor* executor) -> void {
    std::lock_guard const lock_guard{m_mutex};
    m_executor = executor;
}

auto ExecutorHandle::clear() -> void {
    std::lock_guard const lock_guard{m_mutex};
    m_executor = nullptr;
}
}  // namespace spider::worker
