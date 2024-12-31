#ifndef SPIDER_CLIENT_DRIVER_HPP
#define SPIDER_CLIENT_DRIVER_HPP

#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../io/Serializer.hpp"
#include "../worker/FunctionManager.hpp"
#include "Data.hpp"
#include "Job.hpp"
#include "task.hpp"
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
 * replicate the task. TODO: Use the timeout.
 */
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SPIDER_REGISTER_TASK_TIMEOUT(func, timeout) SPIDER_WORKER_REGISTER_TASK(func)

namespace spider {
namespace core {
class MetadataStorage;
class DataStorage;
}  // namespace core

/**
 * An interface for a client to interact with Spider and create jobs, access the kv-store, etc.
 */
class Driver {
public:
    /**
     * @param storage_url
     * @throw spider::ConnectionException
     */
    explicit Driver(std::string const& storage_url);

    /**
     * @param storage_url
     * @param id A caller-specified ID to associate with this driver. All jobs created by this
     * driver will be associated with this ID. This may be useful if, for instance, the caller
     * fails and then needs to reconnect and retrieve all previously created jobs. NOTE: It is
     * undefined behaviour for two clients to concurrently use the same ID.
     * @throw spider::ConnectionException
     * @throw spider::DriverIdInUseException
     */
    Driver(std::string const& storage_url, boost::uuids::uuid id);

    /**
     * @return Data builder.
     */
    template <Serializable T>
    auto get_data_builder() -> Data<T>::Builder {
        typedef typename Data<T>::Builder DataBuilder;
        return DataBuilder{m_data_storage, m_id, DataBuilder::Driver};
    }

    /**
     * Inserts the given key-value pair into the key-value store, overwriting any existing value.
     *
     * @param key
     * @param value
     * @throw spider::ConnectionException
     */
    auto kv_store_insert(std::string const& key, std::string const& value);

    /**
     * Gets the value corresponding to the given key.
     *
     * NOTE: Callers cannot get values created by other clients, but they can get values created by
     * previous `Driver` instances with the same client ID.
     *
     * @param key
     * @return An optional containing the value if the given key exists, or `std::nullopt`
     * otherwise.
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
     * @return  A `TaskGraph` of the inputs bound to `task`.
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
     * @throw spider::ConnectionException
     */
    template <TaskIo ReturnType, TaskIo... Params>
    auto
    start(TaskGraph<ReturnType(Params...)> const& graph, Params&&... inputs) -> Job<ReturnType>;

    /**
     * Gets all scheduled and running jobs started by drivers with the current client's ID.
     *
     * NOTE: This method will not return jobs that have finished.
     *
     * @return IDs of the jobs.
     * @throw spider::ConnectionException
     */
    auto get_jobs() -> std::vector<boost::uuids::uuid>;

private:
    boost::uuids::uuid m_id;
    std::shared_ptr<core::MetadataStorage> m_metadata_storage;
    std::shared_ptr<core::DataStorage> m_data_storage;
    std::thread m_heartbeat_thread;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_DRIVER_HPP
