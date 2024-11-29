#ifndef SPIDER_CLIENT_TASKGRAPH_HPP
#define SPIDER_CLIENT_TASKGRAPH_HPP

#include "task.hpp"

namespace spider {
/**
 * A TaskGraph represents a directed acyclic graph (DAG) of tasks.
 *
 * @tparam ReturnType
 * @tparam Params
 */
template <TaskIo ReturnType, TaskIo... Params>
class TaskGraph {};

}  // namespace spider

#endif  // SPIDER_CLIENT_TASKGRAPH_HPP
