#ifndef SPIDER_CLIENT_TASK_HPP
#define SPIDER_CLIENT_TASK_HPP

#include <memory>

#include "Future.hpp"

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
    /**
     * Runs the task graph.
     *
     * @tparam Args input types of the task graph
     * @tparam R return type of the task graph
     *
     * @param args inputs of the task graph
     * @return future of the result
     */
    auto run(Args&&... args) -> Future<R>;

private:
    std::unique_ptr<TaskGraphImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_TASK_HPP
