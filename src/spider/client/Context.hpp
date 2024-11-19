#ifndef SPIDER_CLIENT_CONTEXT_HPP
#define SPIDER_CLIENT_CONTEXT_HPP

#include <boost/uuid/uuid.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <string>

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
     * @param message error message indicating the reason of failure
     */
    auto abort(std::string const& message);

    /**
     * Gets id of the current running task instance.
     *
     * @return id of the current running task instance.
     */
    [[nodiscard]] auto get_id() const -> boost::uuids::uuid;

    /**
     * Gets data by key. Cannot get data created by other tasks.
     *
     * @tparam T type of the value stored in data
     * @param key key of the data
     * @return std::nullopt if no data with key is stored, the data associated by the key otherwise
     */
    template <typename T>
    auto get_data(std::string const& key) -> std::optional<Data<T>>;

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
     * @tparam Inputs types of task, task graph, spider::Data or POD value
     * @tparam GraphInputs input types of the new task graph
     *
     * @param task child task to be bound on
     * @param inputs task or task graph whose outputs to bind to f, or value or spider::Data used as
     * input
     * @return task graph representing the task dependencies. If none of args is a task or task
     * graph, returns a task graph with only one task
     */
    template <class R, class... Args, class... Inputs, class... GraphInputs>
    auto
    bind(std::function<R(Args...)> const& task, Inputs&&... inputs) -> TaskGraph<R(GraphInputs...)>;

    /**
     * Runs task on Spider.
     *
     * @tparam R return type of the task
     * @tparam Args input types of the task
     *
     * @param task task to run
     * @param args task input
     * @return job representing the running task
     */
    template <class R, class... Args>
    auto run(std::function<R(Args...)> const& task, Args&&... args) -> Job<R>;

    /**
     * Runs task graph on Spider.
     *
     * @tparam R return type of the task graph
     * @tparam Args input types of the task graph
     *
     * @param graph task graph to run
     * @param args task input
     * @return job representing the running task graph
     */
    template <class R, class... Args>
    auto run(TaskGraph<R(Args...)> const& graph, Args&&... args) -> Job<R>;

private:
    std::unique_ptr<ContextImpl> m_impl;
};
}  // namespace spider

#endif // SPIDER_CLIENT_CONTEXT_HPP
