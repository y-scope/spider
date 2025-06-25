#ifndef SPIDER_WORKER_EXECUTORHANDLE_HPP
#define SPIDER_WORKER_EXECUTORHANDLE_HPP

#include <mutex>
#include <optional>

#include <boost/uuid/uuid.hpp>

#include "TaskExecutor.hpp"

namespace spider::worker {
/**
 * This class acts as a handle for thread-safe access to the task executor and task id.
 * It maintains a weak reference to the task executor to prevent multiple destructor calls and
 * ensures that access remains valid only while the executor itself is valid.
 */
class ExecutorHandle {
public:
    [[nodiscard]] auto get_task_id() -> std::optional<boost::uuids::uuid>;
    [[nodiscard]] auto get_executor() -> TaskExecutor*;
    auto executor_cancel() -> void;
    auto set(boost::uuids::uuid task_id, TaskExecutor* executor) -> void;
    auto clear() -> void;

private:
    boost::uuids::uuid m_task_id;

    // Do not use std::shared_ptr to avoid calling destructor twice.
    TaskExecutor* m_executor = nullptr;

    std::mutex m_mutex;
};
}  // namespace spider::worker

#endif
