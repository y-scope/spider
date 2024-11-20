#ifndef SPIDER_CLIENT_CONTEXT_HPP
#define SPIDER_CLIENT_CONTEXT_HPP

#include <boost/uuid/uuid.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../core/Serializer.hpp"
#include "Data.hpp"
#include "Job.hpp"
#include "TaskGraph.hpp"

namespace spider {
class ContextImpl;

class Context {
public:
    /**
     * Aborts the current running task and job. This function never returns.
     *
     * @param message Error message indicating the reason for the abort.
     */
    auto abort(std::string const& message);

    /**
     * @return ID of the current running task instance.
     */
    [[nodiscard]] auto get_id() const -> boost::uuids::uuid;

    /**
     * Gets data by key.
     *
     * NOTE: Callers cannot get data created by other tasks, but they can get data created by
     * previous instances of the same task.
     *
     * @tparam T Type of the value stored in data
     * @param key Key of the data.
     * @return An optional containing the data if the given key exists, or `std::nullopt` otherwise.
     */
    template <Serializable T>
    auto get_data(std::string const& key) -> std::optional<Data<T>>;

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
     * NOTE: Callers cannot get values created by other tasks, but they can get values created by
     * previous instances of the same task.
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
     * @tparam R return type of the task or task graph
     * @tparam Args input types of task or task graph
     * @tparam Inputs types of task, task graph, spider::Data or POD value
     * @tparam GraphInputs input types of the new task graph
     *
     * @param task child task to be bound on
     * @param inputs task or task graph whose outputs to bind to f, or value or spider::Data used as
     * input
     * @return task graph representing the task dependencies. If none of args is a task or task
     * graph, returns a task graph with only one task
     */
    template <Serializable R, Serializable... Args, class... Inputs, class... GraphInputs>
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
     * Gets all jobs started by the task.
     *
     * @return ids of the jobs
     */
    auto get_jobs() -> std::vector<boost::uuids::uuid>;

private:
    std::unique_ptr<ContextImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_CONTEXT_HPP
