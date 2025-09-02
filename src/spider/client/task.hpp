#ifndef SPIDER_CLIENT_TASK_HPP
#define SPIDER_CLIENT_TASK_HPP

#include <type_traits>

#include <spider/client/Data.hpp>
#include <spider/client/type_utils.hpp>
#include <spider/io/Serializer.hpp>

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
using TaskFunction = ReturnType (*)(TaskContext&, TaskParams...);

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

template <class...>
struct ConcatTaskGraphType;

template <TaskIo GraphReturnType, TaskIo... GraphParams>
struct ConcatTaskGraphType<TaskGraph<GraphReturnType, GraphParams...>> {
    using Type = TaskGraph<GraphReturnType, GraphParams...>;
};

template <TaskIo GraphReturnType, TaskIo... GraphParams, class InputType>
struct ConcatTaskGraphType<TaskGraph<GraphReturnType, GraphParams...>, InputType> {
    using Type = TaskGraph<GraphReturnType, GraphParams...>;
};

template <TaskIo GraphReturnType, TaskIo... GraphParams, TaskIo ReturnType, TaskIo... TaskParams>
struct ConcatTaskGraphType<
        TaskGraph<GraphReturnType, GraphParams...>,
        TaskFunction<ReturnType, TaskParams...>
> {
    using Type = TaskGraph<GraphReturnType, GraphParams..., TaskParams...>;
};

template <TaskIo GraphReturnType, TaskIo... GraphParams, TaskIo ReturnType, TaskIo... TaskParams>
struct ConcatTaskGraphType<
        TaskGraph<GraphReturnType, GraphParams...>,
        TaskGraph<ReturnType, TaskParams...>
> {
    using Type = TaskGraph<GraphReturnType, GraphParams..., TaskParams...>;
};

template <class...>
struct MergeTaskGraphTypes;

template <TaskIo GraphReturnType, TaskIo... GraphParams>
struct MergeTaskGraphTypes<TaskGraph<GraphReturnType, GraphParams...>> {
    using Type = TaskGraph<GraphReturnType, GraphParams...>;
};

template <TaskIo ReturnType, TaskIo... GraphParams, class InputType, RunnableOrTaskIo... Inputs>
struct MergeTaskGraphTypes<TaskGraph<ReturnType, GraphParams...>, InputType, Inputs...> {
    using Type = typename MergeTaskGraphTypes<
            typename ConcatTaskGraphType<
                    TaskGraph<ReturnType, GraphParams...>,
                    std::remove_cvref_t<InputType>
            >::Type,
            Inputs...
    >::Type;
};

template <TaskIo ReturnType, RunnableOrTaskIo... Inputs>
using TaskGraphType = typename MergeTaskGraphTypes<TaskGraph<ReturnType>, Inputs...>::Type;
}  // namespace spider

#endif  // SPIDER_CLIENT_TASK_HPP
