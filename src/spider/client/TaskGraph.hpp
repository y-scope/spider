#ifndef SPIDER_CLIENT_TASKGRAPH_HPP
#define SPIDER_CLIENT_TASKGRAPH_HPP

#include <memory>
#include <utility>

#include "task.hpp"

namespace spider {
namespace core {
class TaskGraphImpl;
}  // namespace core

class Driver;
class TaskContext;

/**
 * A TaskGraph represents a directed acyclic graph (DAG) of tasks.
 *
 * @tparam ReturnType
 * @tparam Params
 */
template <TaskIo ReturnType, TaskIo... Params>
class TaskGraph {
private:
    explicit TaskGraph(std::unique_ptr<core::TaskGraphImpl> impl) : m_impl{std::move(impl)} {}

    [[nodiscard]] auto get_impl() const -> core::TaskGraphImpl const& { return *m_impl; }

    std::unique_ptr<core::TaskGraphImpl> m_impl;

    friend class core::TaskGraphImpl;
    friend class Driver;
    friend class TaskContext;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_TASKGRAPH_HPP
