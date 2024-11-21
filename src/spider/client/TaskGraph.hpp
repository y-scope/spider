#ifndef SPIDER_CLIENT_TASKGRAPH_CPP
#define SPIDER_CLIENT_TASKGRAPH_CPP

#include <memory>

#include "Concepts.hpp"
#include "TaskContext.hpp"

namespace spider {

template <TaskIo ReturnType, TaskIo... TaskParams>
using TaskFunction = std::function<ReturnType(TaskContext, TaskParams...)>;

class TaskGraphImpl;

/**
 * TaskGraph represents a DAG of tasks.
 *
 * @tparam ReturnType
 * @tparam Params
 */
template <TaskIo ReturnType, TaskIo... Params>
class TaskGraph {
public:
private:
    std::unique_ptr<TaskGraphImpl> m_impl;
};

template <class T>
concept Runnable = cIsSpecializationV<T, TaskFunction> || cIsSpecializationV<T, TaskGraph>;

template <class T>
concept RunnableOrTaskIo = Runnable<T> || TaskIo<T>;

}  // namespace spider

#endif  // SPIDER_CLIENT_TASKGRAPH_CPP
