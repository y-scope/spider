#ifndef SPIDER_CLIENT_TASKGRAPH_HPP
#define SPIDER_CLIENT_TASKGRAPH_HPP

#include <functional>
#include <memory>

#include "Concepts.hpp"

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

// Forward declare `TaskContext` since `TaskFunction` takes `TaskContext` as a param, and
// `TaskContext` uses `TaskFunction` as a param in its methods.
class TaskContext;

/**
 * A function that can be run as a task on Spider.
 *
 * @tparam ReturnType
 * @tparam TaskParams
 */
template <TaskIo ReturnType, TaskIo... TaskParams>
using TaskFunction = std::function<ReturnType(TaskContext, TaskParams...)>;

/**
 * Concept for an object that's runnable on Spider.
 *
 * @tparam T
 */
template <typename T>
concept Runnable = cIsSpecializationV<T, TaskFunction> || cIsSpecializationV<T, TaskGraph>;

template <typename T>
concept RunnableOrTaskIo = Runnable<T> || TaskIo<T>;
}  // namespace spider

#endif  // SPIDER_CLIENT_TASKGRAPH_HPP
