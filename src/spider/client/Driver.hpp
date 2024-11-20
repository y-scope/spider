#ifndef SPIDER_CLIENT_DRIVER_HPP
#define SPIDER_CLIENT_DRIVER_HPP

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../core/Serializer.hpp"
#include "../worker/FunctionManager.hpp"
#include "Data.hpp"
#include "Job.hpp"
#include "TaskGraph.hpp"

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
/**
 * Registers function to Spider
 * @param func function to register
 */
#define SPIDER_REGISTER_TASK(func) SPIDER_WORKER_REGISTER_TASK(func)

/**
 * Registers function to Spider with timeout
 * @param func function to register
 * @param timeout task is considered straggler after timeout ms, and Spider triggers replicating
 * the task
 */
#define SPIDER_REGISTER_TASK_TIMEOUT(func, timeout) SPIDER_WORKER_REGISTER_TASK(func)

// NOLINTEND(cppcoreguidelines-macro-usage)

namespace spider {
class DriverImpl;

class Driver {
public:
    /**
     * Create a spider driver that connects to a storage.
     *
     * @param url storage url
     */
    explicit Driver(std::string const& url);

    /**
     * Create a spider driver that connects to a storage.
     *
     * @param url storage url
     * @param id client id
     */
    Driver(std::string const& url, boost::uuids::uuid id);

    /**
     * Gets data by key.
     *
     * @tparam T type of the value stored in data
     * @param key key of the data
     * @return std::nullopt if no data with key is stored, the data associated by the key otherwise
     */
    template <Serializable T>
    auto get_data(std::string const& key) -> std::optional<spider::Data<T>>;

    /**
     * Insert the key-value pair into the key value store. Overwrite the existing value stored if
     * key already exists.
     * @param key key of the key-value pair
     * @param value value of the key-value pair
     */
    auto insert_kv(std::string const& key, std::string const& value);

    /**
     * Get the value based on the key. Client can only get the value created by itself.
     * @param key key to lookup
     * @return std::nullopt if key not in storage, corresponding value if key in storage
     */
    auto get_kv(std::string const& key) -> std::optional<std::string>;

    /**
     * Binds inputs to a task. Input of the task can be bound from outputs of task or task graph,
     * forming dependencies between tasks. Input can also be a value or a spider::Data.
     *
     * @tparam R return type of the task or task graph
     * @tparam Args input types of task or task graph
     * @tparam Inputs types of task, task graph, spider::Data or Serializable
     * @tparam GraphInputs input types of the new task graph
     *
     * @param task child task to be bound on
     * @param inputs task or task graph whose outputs to bind to f, or value or spider::Data used as
     * input
     * @return task graph representing the task dependencies. If none of args is a task or task
     * graph, returns a task graph with only one task
     */
    template <Serializable R, Serializable... Args, class... Inputs, Serializable... GraphInputs>
    auto
    bind(std::function<R(Args...)> const& task, Inputs&&... inputs) -> TaskGraph<R(GraphInputs...)>;

    /**
     * Starts task on Spider.
     *
     * @tparam R return type of the task
     * @tparam Args input types of the task
     *
     * @param task task to run
     * @param args task input
     * @return job representing the running task
     */
    template <Serializable R, Serializable... Args>
    auto start(std::function<R(Args...)> const& task, Args&&... args) -> Job<R>;

    /**
     * Starts task graph on Spider.
     *
     * @tparam R return type of the task graph
     * @tparam Args input types of the task graph
     *
     * @param graph task graph to run
     * @param args task input
     * @return job representing the running task graph
     */
    template <Serializable R, Serializable... Args>
    auto start(TaskGraph<R(Args...)> const& graph, Args&&... args) -> Job<R>;

    /**
     * Gets all jobs started by drivers with same client id.
     *
     * @return ids of the jobs
     */
    auto get_jobs() -> std::vector<boost::uuids::uuid>;

private:
    std::unique_ptr<DriverImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_DRIVER_HPP
