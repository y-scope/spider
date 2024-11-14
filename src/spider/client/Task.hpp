/**
 * Task.hpp include functions that can be called inside a Task.
 */

#ifndef SPIDER_CORE_SPIDER_HPP
#define SPIDER_CORE_SPIDER_HPP

#include <functional>
#include <optional>
#include <string>

#include "Data.hpp"
#include "Job.hpp"
#include "TaskGraph.hpp"

namespace spider {
/**
 * Gets data by key.
 * This function can be called by a client to get all data or called by a task to get data created
 * by it.
 * @tparam T type of the value stored in data
 * @param key key of the data
 * @return std::nullopt if no data with key is stored, the data associated by the key otherwise
 */
template <typename T>
auto get_data(std::string const& key) -> std::optional<spider::Data<T>>;

/**
 * Add task as a child of current task.
 * This function can only be called by a task.
 * @tparam F task graph type or function type for a single task
 * @param f child task or task graph
 */
template <class F>
void add_child(F const& f);

/**
 * Binds inputs to a task. Input of the task can be bound from outputs of task or task graph,
 * forming dependencies between tasks. Input can also be a value or a spider::Data.
 * This function can be called by a client or by a task.
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
        -> spider::TaskGraph<R(GraphInputs...)>;

/**
 * Runs task on Spider.
 * This function can be called by a client or by a task.
 *
 * @tparam R return type of the task
 * @tparam Args input types of the task
 *
 * @param task task to run
 * @param args task input
 * @return future of the result
 */
template <class R, class... Args>
auto run(std::function<R(Args...)> const& task, Args&&... args) -> Job<R>;

/**
 * Runs task graph on Spider.
 * This function can be called by a client or by a task.
 *
 * @tparam R return type of the task graph
 * @tparam Args input types of the task graph
 *
 * @param graph task graph to run
 * @param args task input
 * @return future of the result
 */
template <class R, class... Args>
auto run(TaskGraph<R(Args...)> const& graph, Args&&... args) -> Job<R>;
}  // namespace spider

#endif
