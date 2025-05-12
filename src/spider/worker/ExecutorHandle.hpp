#ifndef SPIDER_WORKER_EXECUTORHANDLE_HPP
#define SPIDER_WORKER_EXECUTORHANDLE_HPP

#include <mutex>

#include <boost/uuid/uuid.hpp>

#include "TaskExecutor.hpp"

namespace spider::worker {
/**
 * Provides a thread-safe access to the task executor and other task related variables across
 * threads.
 */
class ExecutorHandle {
public:
    [[nodiscard]] auto get_task_id() -> std::optional<boost::uuids::uuid>;
    [[nodiscard]] auto get_executor() -> TaskExecutor*;
    auto set(boost::uuids::uuid task_id, TaskExecutor* executor) -> void;
    auto clear() -> void;

private:
    boost::uuids::uuid m_task_id;  // The task id is only valid if there is an executor.
    TaskExecutor* m_executor
            = nullptr;  // Do not use std::shared_ptr to avoid calling destructor twice.
    std::mutex m_mutex;
};
}  // namespace spider::worker

#endif
