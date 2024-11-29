#ifndef SPIDER_CLIENT_TASKCONTEXT_HPP
#define SPIDER_CLIENT_TASKCONTEXT_HPP

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "Job.hpp"
#include "task.hpp"
#include "TaskGraph.hpp"

namespace spider {
class ContextImpl;

/**
 * TaskContext provides a task with all Spider functionalities, e.g. getting task instance id,
 * accessing data storage, creating and waiting for new jobs, etc.
 * TaskContext is provided as first argument to a task.
 */
class TaskContext {
public:
    /**
     * Aborts the current task and job. This function never returns.
     *
     * @param message The reason for the abort.
     *
     * @throw spider::ConnectionException
     */
    [[noreturn]] auto abort(std::string const& message) -> void;

    /**
     * @return ID of the current running task instance.
     */
    [[nodiscard]] auto get_id() const -> boost::uuids::uuid;

    /**
     * Inserts the given key-value pair into the key-value store, overwriting any existing value.
     *
     * @param key
     * @param value
     *
     * @throw spider::ConnectionException
     */
    auto kv_store_insert(std::string const& key, std::string const& value) -> void;

    /**
     * Gets the value corresponding to the given key.
     *
     * NOTE: Callers cannot get values created by other tasks, but they can get values created by
     * previous instances of the same task.
     *
     * @param key
     * @return An optional containing the value if the given key exists, or `std::nullopt`
     * otherwise.
     *
     * @throw spider::ConnectionException
     */
    auto kv_store_get(std::string const& key) -> std::optional<std::string>;

    /**
     * Binds inputs to a task. Inputs can be:
     * - the outputs of a task or task graph, forming dependencies between tasks.
     * - any value that satisfies the `TaskIo` concept.
     *
     * @tparam ReturnType Return type for both the task and the resulting `TaskGraph`.
     * @tparam TaskParams
     * @tparam Inputs
     * @tparam GraphParams
     * @param task
     * @param inputs Inputs to bind to `task`. If an input is a `Task` or `TaskGraph`, their
     * outputs will be bound to the inputs of `task`.
     * @return A `TaskGraph` of the inputs bound to `task`.
     */
    template <
            TaskIo ReturnType,
            TaskIo... TaskParams,
            RunnableOrTaskIo... Inputs,
            TaskIo... GraphParams>
    auto bind(TaskFunction<ReturnType, TaskParams...> const& task, Inputs&&... inputs)
            -> TaskGraph<ReturnType(GraphParams...)>;

    /**
     * Starts running a task with the given inputs on Spider.
     *
     * @tparam ReturnType
     * @tparam Params
     * @param task
     * @param inputs
     * @return A job representing the running task.
     *
     * @throw spider::ConnectionException
     */
    template <TaskIo ReturnType, TaskIo... Params>
    auto
    start(TaskFunction<ReturnType, Params...> const& task, Params&&... inputs) -> Job<ReturnType>;

    /**
     * Starts running a task graph with the given inputs on Spider.
     *
     * @tparam ReturnType
     * @tparam Params
     * @param graph
     * @param inputs
     * @return A job representing the running task graph.
     *
     * @throw spider::ConnectionException
     */
    template <TaskIo ReturnType, TaskIo... Params>
    auto
    start(TaskGraph<ReturnType(Params...)> const& graph, Params&&... inputs) -> Job<ReturnType>;

    /**
     * Gets all jobs started by this task.
     *
     * @return IDs of the jobs.
     *
     * @throw spider::ConnectionException
     */
    auto get_jobs() -> std::vector<boost::uuids::uuid>;

private:
    std::unique_ptr<ContextImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_TASKCONTEXT_HPP
