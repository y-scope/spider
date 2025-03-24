#ifndef SPIDER_CLIENT_DRIVER_HPP
#define SPIDER_CLIENT_DRIVER_HPP

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <fmt/format.h>

#include "../core/Error.hpp"
#include "../core/TaskGraphImpl.hpp"
#include "../io/Serializer.hpp"
#include "../storage/JobSubmissionBatch.hpp"
#include "../storage/StorageConnection.hpp"
#include "../storage/StorageFactory.hpp"
#include "../worker/FunctionManager.hpp"
#include "../worker/FunctionNameManager.hpp"
#include "Data.hpp"
#include "Exception.hpp"
#include "Job.hpp"
#include "task.hpp"
#include "TaskGraph.hpp"
#include "utility"

/**
 * Registers a Task function with Spider
 * @param func
 */
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SPIDER_REGISTER_TASK(func) \
    SPIDER_WORKER_REGISTER_TASK(func) SPIDER_WORKER_REGISTER_TASK_NAME(func)

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
class Task;
class TaskGraph;
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
        using DataBuilder = typename Data<T>::Builder;
        return DataBuilder{
                m_data_storage,
                m_id,
                DataBuilder::DataSource::Driver,
                m_storage_factory,
                m_conn
        };
    }

    /**
     * Inserts the given key-value pair into the key-value store, overwriting any existing value.
     *
     * @param key
     * @param value
     * @throw spider::ConnectionException
     */
    auto kv_store_insert(std::string const& key, std::string const& value) -> void;

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
     * @param task
     * @param inputs Inputs to bind to `task`. If an input is a `Task` or `TaskGraph`, their
     * outputs will be bound to the inputs of `task`.
     * @return  A `TaskGraph` of the inputs bound to `task`.
     */
    template <TaskIo ReturnType, TaskIo... TaskParams, RunnableOrTaskIo... Inputs>
    auto bind(TaskFunction<ReturnType, TaskParams...> const& task, Inputs&&... inputs)
            -> TaskGraphType<ReturnType, Inputs...> {
        std::optional<core::TaskGraphImpl> optional_graph
                = core::TaskGraphImpl::bind(task, std::forward<Inputs>(inputs)...);
        if (!optional_graph.has_value()) {
            throw std::invalid_argument("Failed to bind inputs to task.");
        }
        std::unique_ptr<core::TaskGraphImpl> graph
                = std::make_unique<core::TaskGraphImpl>(std::move(optional_graph.value()));

        return TaskGraphType<ReturnType, Inputs...>{std::move(graph)};
    }

    /**
     * Begins a batch of `start` calls. This allows the driver to submit multiple jobs in a single
     * batch, which can be more efficient than submitting jobs individually.
     *
     * Needs to be paired with `end_batch_start`.
     *
     * If a batch has already been started, this method is a no-op.
     */
    auto begin_batch_start() -> void {
        if (nullptr != m_batch) {
            return;
        }
        m_batch = m_storage_factory->provide_job_submission_batch(*m_conn);
    }

    /**
     * Ends a batch of `start` calls. This submits all jobs in the batch to Spider.
     *
     * @throw spider::ConnectionException
     */
    auto end_batch_start() -> void {
        if (nullptr == m_batch) {
            return;
        }
        core::StorageErr const err = m_batch->submit_batch(*m_conn);
        m_batch = nullptr;
        if (!err.success()) {
            throw ConnectionException(fmt::format("Failed to start job: {}", err.description));
        }
    }

    /**
     * Starts running a task with the given inputs on Spider.
     *
     * @tparam ReturnType
     * @tparam Params
     * @tparam Inputs
     * @param task
     * @param inputs
     * @return A job representing the running task.
     * @throw spider::ConnectionException
     */
    template <TaskIo ReturnType, TaskIo... Params, TaskIo... Inputs>
    auto
    start(TaskFunction<ReturnType, Params...> const& task, Inputs&&... inputs) -> Job<ReturnType> {
        // Check input type
        static_assert(
                sizeof...(Inputs) == sizeof...(Params),
                "Number of inputs must match number of parameters."
        );
        for_n<sizeof...(Inputs)>([&](auto i) {
            using InputType = std::tuple_element_t<i.cValue, std::tuple<Inputs...>>;
            using ParamType = std::tuple_element_t<i.cValue, std::tuple<Params...>>;
            static_assert(
                    std::is_same_v<std::remove_cvref_t<InputType>, std::remove_cvref_t<ParamType>>,
                    "Input type does not match parameter type."
            );
        });

        std::optional<core::Task> optional_task = core::TaskGraphImpl::create_task(task);
        if (!optional_task.has_value()) {
            throw std::invalid_argument("Failed to create task.");
        }
        core::Task& new_task = optional_task.value();
        if (!core::TaskGraphImpl::task_add_input(new_task, std::forward<Inputs>(inputs)...)) {
            throw std::invalid_argument("Failed to add inputs to task.");
        }
        boost::uuids::random_generator gen;
        boost::uuids::uuid const job_id = gen();
        core::TaskGraph graph;
        graph.add_task(new_task);
        graph.add_input_task(new_task.get_id());
        graph.add_output_task(new_task.get_id());
        if (nullptr != m_batch) {
            core::StorageErr const err
                    = m_metadata_storage->add_job_batch(*m_conn, *m_batch, job_id, m_id, graph);
            if (!err.success()) {
                throw ConnectionException(fmt::format("Failed to start job: {}", err.description));
            }
        } else {
            core::StorageErr const err = m_metadata_storage->add_job(*m_conn, job_id, m_id, graph);
            if (!err.success()) {
                throw ConnectionException(fmt::format("Failed to start job: {}", err.description));
            }
        }

        return Job<ReturnType>{
                job_id,
                m_metadata_storage,
                m_data_storage,
                m_storage_factory,
                m_conn
        };
    }

    /**
     * Starts running a task graph with the given inputs on Spider.
     *
     * @tparam ReturnType
     * @tparam Params
     * @tparam Inputs
     * @param graph
     * @param inputs
     * @return A job representing the running task graph.
     * @throw spider::ConnectionException
     */
    template <TaskIo ReturnType, TaskIo... Params, TaskIo... Inputs>
    auto
    start(TaskGraph<ReturnType, Params...> const& graph, Inputs&&... inputs) -> Job<ReturnType> {
        // Check input type
        static_assert(
                sizeof...(Inputs) == sizeof...(Params),
                "Number of inputs must match number of parameters."
        );
        for_n<sizeof...(Inputs)>([&](auto i) {
            using InputType = std::tuple_element_t<i.cValue, std::tuple<Inputs...>>;
            using ParamType = std::tuple_element_t<i.cValue, std::tuple<Params...>>;
            static_assert(
                    std::is_same_v<std::remove_cvref_t<InputType>, std::remove_cvref_t<ParamType>>,
                    "Input type does not match parameter type."
            );
        });

        if (!graph.m_impl->add_inputs(std::forward<Inputs>(inputs)...)) {
            throw std::invalid_argument("Failed to add inputs to task graph.");
        }
        // Reset ids in case the same graph is submitted before
        graph.m_impl->reset_ids();
        boost::uuids::random_generator gen;
        boost::uuids::uuid const job_id = gen();
        if (nullptr != m_batch) {
            core::StorageErr const err = m_metadata_storage->add_job_batch(
                    *m_conn,
                    *m_batch,
                    job_id,
                    m_id,
                    graph.m_impl->get_graph()
            );
            if (!err.success()) {
                throw ConnectionException(fmt::format("Failed to start job: {}", err.description));
            }
        } else {
            core::StorageErr const err
                    = m_metadata_storage->add_job(*m_conn, job_id, m_id, graph.m_impl->get_graph());
            if (!err.success()) {
                throw ConnectionException(fmt::format("Failed to start job: {}", err.description));
            }
        }

        return Job<ReturnType>{
                job_id,
                m_metadata_storage,
                m_data_storage,
                m_storage_factory,
                m_conn
        };
    }

    /**
     * Gets all scheduled and running jobs started by drivers with the current client's ID.
     *
     * NOTE: This method will not return jobs that have finished.
     *
     * @return IDs of the jobs.
     * @throw spider::ConnectionException
     */
    auto get_jobs() -> std::vector<boost::uuids::uuid> {
        std::vector<boost::uuids::uuid> job_ids;
        core::StorageErr const err
                = m_metadata_storage->get_jobs_by_client_id(*m_conn, m_id, &job_ids);
        if (!err.success()) {
            throw ConnectionException("Failed to get jobs.");
        }
        return job_ids;
    }

private:
    boost::uuids::uuid m_id;
    std::shared_ptr<core::MetadataStorage> m_metadata_storage;
    std::shared_ptr<core::DataStorage> m_data_storage;
    std::shared_ptr<core::StorageFactory> m_storage_factory;
    std::shared_ptr<core::StorageConnection> m_conn;
    std::shared_ptr<core::JobSubmissionBatch> m_batch{nullptr};
    std::jthread m_heartbeat_thread;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_DRIVER_HPP
