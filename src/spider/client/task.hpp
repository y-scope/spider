#ifndef SPIDER_CLIENT_TASK_HPP
#define SPIDER_CLIENT_TASK_HPP

#include "../core/Serializer.hpp"
#include "Data.hpp"
#include "TaskContext.hpp"
#include "TaskGraph.hpp"
#include "type_utils.hpp"

namespace spider {

/**
 * Concept that represents the input to or output from a Task.
 *
 * @tparam T
 */
template <class T>
concept TaskIo = Serializable<T> || cIsSpecializationV<T, Data>;

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

#endif  // SPIDER_CLIENT_TASK_HPP
