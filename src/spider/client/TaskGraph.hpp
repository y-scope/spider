#ifndef SPIDER_CLIENT_TASKGRAPH_CPP
#define SPIDER_CLIENT_TASKGRAPH_CPP

#include <memory>

#include "../core/Serializer.hpp"

namespace spider {
class TaskGraphImpl;

/**
 * TaskGraph represents a DAG of tasks.
 * @tparam R return type of the task graph
 * @tparam Args input types of the task graph
 */
template <Serializable R, Serializable... Args>
class TaskGraph {
public:
private:
    std::unique_ptr<TaskGraphImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_TASKGRAPH_CPP
