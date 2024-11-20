#ifndef SPIDER_CLIENT_SPIDER_HPP
#define SPIDER_CLIENT_SPIDER_HPP

#include <functional>
#include <optional>
#include <string>

// IWYU pragma: begin_exports
#include "Context.hpp"
#include "Data.hpp"
#include "Job.hpp"
#include "TaskGraph.hpp"

// IWYU pragma: end_exports

namespace spider {
/**
 * Initializes Spider library
 */
void init();

/**
 * Connects to storage
 * @param url url of the storage to connect
 */
void connect(std::string const& url);

/**
 * Registers function to Spider
 * @param function function to register
 */
template <class R, class... Args>
void register_task(std::function<R(Args...)> const& function);

/**
 * Registers function to Spider with timeout
 * @param function function to register
 * @param timeout task is considered straggler after timeout ms, and Spider triggers replicate the
 * task
 */
template <class R, class... Args>
void register_task(std::function<R(Args...)> const& function, float timeout);

/**
 * Gets data by key.
 *
 * @tparam T type of the value stored in data
 * @param key key of the data
 * @return std::nullopt if no data with key is stored, the data associated by the key otherwise
 */
template <typename T>
auto get_data(std::string const& key) -> std::optional<spider::Data<T>>;

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
 * @return task graph representing the task dependencies. If none of args is a task or task graph,
 * returns a task graph with only one task
 */
template <class R, class... Args, class... Inputs, class... GraphInputs>
auto bind(std::function<R(Args...)> const& task, Inputs&&... inputs)
        -> TaskGraph<R(GraphInputs...)>;

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

}  // namespace spider

#endif  // SPIDER_CLIENT_SPIDER_HPP
