#ifndef SPIDER_CLIENT_TASK_HPP
#define SPIDER_CLIENT_TASK_HPP

#include <functional>

#include "../utils/Serializer.hpp"
#include "Data.hpp"
#include "type_utils.hpp"

namespace spider {

/**
 * Concept that represents the input to or output from a Task.
 *
 * @tparam T
 */
template <class T>
concept TaskIo = Serializable<T> || cIsSpecializationV<T, Data>;

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

// Forward declare `TaskGraph` since `Runnable` takes `TaskGraph` as a param, and `TaskGraph` uses
// `TaskIo` defined in this header as its template params.
template <TaskIo ReturnType, TaskIo... Params>
class TaskGraph;

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

#endif  // SPIDER_CLIENT_TASK_HPP
