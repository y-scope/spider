#ifndef SPIDER_CLIENT_TASKGRAPH_HPP
#define SPIDER_CLIENT_TASKGRAPH_HPP

#include <memory>

#include "task.hpp"

namespace spider {
class TaskGraphImpl;

/**
 * A TaskGraph represents a directed acyclic graph (DAG) of tasks.
 *
 * @tparam ReturnType
 * @tparam Params
 */
template <TaskIo ReturnType, TaskIo... Params>
class TaskGraph {
private:
    std::unique_ptr<TaskGraphImpl> m_impl;
};

}  // namespace spider

#endif  // SPIDER_CLIENT_TASKGRAPH_HPP
