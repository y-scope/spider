#ifndef SPIDER_CLIENT_TASKGRAPH_CPP
#define SPIDER_CLIENT_TASKGRAPH_CPP

#include <memory>

#include "Concepts.hpp"

namespace spider {
class TaskGraphImpl;

/**
 * TaskGraph represents a DAG of tasks.
 *
 * @tparam ReturnType
 * @tparam Params
 */
template <TaskArgument ReturnType, TaskArgument... Params>
class TaskGraph {
public:
private:
    std::unique_ptr<TaskGraphImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_TASKGRAPH_CPP
