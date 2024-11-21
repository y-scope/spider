#ifndef SPIDER_CLIENT_DRIVER_HPP
#define SPIDER_CLIENT_DRIVER_HPP

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../worker/FunctionManager.hpp"
#include "Concepts.hpp"
#include "Job.hpp"
#include "TaskGraph.hpp"

/**
 * Registers a Task function with Spider
 * @param func
 */
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SPIDER_REGISTER_TASK(func) SPIDER_WORKER_REGISTER_TASK(func)

/**
 * Registers a timed Task function with Spider
 * @param func
 * @param timeout The time after which the task is considered a straggler, triggering Spider to
 * replicate the task.
 */
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SPIDER_REGISTER_TASK_TIMEOUT(func, timeout) SPIDER_WORKER_REGISTER_TASK(func)

namespace spider {
class DriverImpl;

/**
 * Driver provides Spider functionalities for a client, e.g. accessing data storage, creating new
 * jobs.
 */
class Driver {
public:
    /**
     * @param storage_url
     */
    explicit Driver(std::string const& storage_url);

    /**
     * @param storage_url
     * @param id User could provide client id to access the jobs and data created from a previous
     * Driver with same id
     */
    Driver(std::string const& storage_url, boost::uuids::uuid id);

    /**
     * Inserts the given key-value pair into the key-value store, overwriting any existing value.
     *
     * @param key
     * @param value
     */
    auto insert_kv(std::string const& key, std::string const& value);

    /**
     * Gets the value corresponding to the given key.
     *
     * NOTE: Callers cannot get values created by other clients, but they can get values created by
     * previous `Driver` with the same client id
     *
     * @param key
     * @return An optional containing the value if the given key exists, or `std::nullopt`
     * otherwise.
     */
    auto get_kv(std::string const& key) -> std::optional<std::string>;

    /**
     * Binds inputs to a task. Input of the task can be bound from outputs of task or task graph,
     * forming dependencies between tasks. Input can also be a value or a spider::Data.
     *
     * @tparam ReturnType Return type for both the task and the resulting `TaskGraph`.
     * @tparam TaskParams
     * @tparam Inputs
     * @tparam GraphParams
     *
     * @param task
     * @param inputs Inputs to bind to `task`. If an input is a `Task` or `TaskGraph`, their
     * outputs will be bound to the inputs of `task`.
     * @return  A `TaskGraph` of the inputs bound to `task`.
     */
    template <
            TaskIo ReturnType,
            TaskIo... TaskParams,
            class... Inputs,
            TaskIo... GraphParams>
    auto bind(std::function<ReturnType(TaskParams...)> const& task, Inputs&&... inputs)
            -> TaskGraph<ReturnType(GraphParams...)>;

    /**
     * Starts running a task with the given inputs on Spider.
     *
     * @tparam ReturnType
     * @tparam Params
     *
     * @param task task to run
     * @param inputs task input
     * @return A job representing the running task
     */
    template <TaskIo ReturnType, TaskIo... Params>
    auto
    start(std::function<ReturnType(Params...)> const& task, Params&&... inputs) -> Job<ReturnType>;

    /**
     * Starts running a task graph with the given inputs on Spider.
     *
     * @tparam ReturnType
     * @tparam Params input types of the task grap
     *
     * @param graph
     * @param inputs
     * @return A job representing the running task graph
     */
    template <TaskIo ReturnType, TaskIo... Params>
    auto
    start(TaskGraph<ReturnType(Params...)> const& graph, Params&&... inputs) -> Job<ReturnType>;

    /**
     * Gets all jobs started by drivers with same client id.
     *
     * @return IDs of the jobs
     */
    auto get_jobs() -> std::vector<boost::uuids::uuid>;

private:
    std::unique_ptr<DriverImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_DRIVER_HPP
