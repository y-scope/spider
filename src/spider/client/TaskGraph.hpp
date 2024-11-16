#ifndef SPIDER_CLIENT_TASK_HPP
#define SPIDER_CLIENT_TASK_HPP

#include <memory>

namespace spider {
class TaskGraphImpl;

/**
 * TaskGraph represents a DAG of tasks.
 * @tparam R return type of the task graph
 * @tparam Args input types of the task graph
 */
template <class R, class... Args>
class TaskGraph {
public:
private:
    std::unique_ptr<TaskGraphImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_TASK_HPP
