#ifndef SPIDER_WORKER_EXECUTORHANDLE_HPP
#define SPIDER_WORKER_EXECUTORHANDLE_HPP

#include <mutex>
#include <optional>

#include <boost/uuid/uuid.hpp>

#include "TaskExecutor.hpp"

namespace spider::worker {
/**
 * This singleton class acts as a handle for thread-safe access to the task executor and task id.
 * It maintains a weak reference to the task executor to prevent multiple destructor calls and
 * ensures that access remains valid only while the executor itself is valid.
 */
class ExecutorHandle {
public:
    [[nodiscard]] static auto get_task_id() -> std::optional<boost::uuids::uuid>;
    static auto cancel_executor() -> void;
    static auto set(TaskExecutor* executor) -> void;
    static auto clear() -> void;

    // Delete default constructor
    ExecutorHandle() = delete;
    // Delete copy constructor and assignment operator
    ExecutorHandle(ExecutorHandle const&) = delete;
    auto operator=(ExecutorHandle const&) -> ExecutorHandle& = delete;
    // Delete move constructor and assignment operator
    ExecutorHandle(ExecutorHandle&&) = delete;
    auto operator=(ExecutorHandle&&) -> ExecutorHandle& = delete;
    // Default destructor
    ~ExecutorHandle() = default;

private:
    // Do not use std::shared_ptr to avoid calling destructor twice.
    static TaskExecutor* m_executor;

    static std::mutex m_mutex;
};
}  // namespace spider::worker

#endif
