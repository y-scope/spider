#ifndef SPIDER_CORE_TASKGRAPHIMPL_HPP
#define SPIDER_CORE_TASKGRAPHIMPL_HPP

#include "../client/task.hpp"
#include "../core/Task.hpp"
#include "../core/TaskGraph.hpp"
#include "../worker/FunctionManager.hpp"

namespace spider::core {

class TaskGraphImpl {
public:
    template <TaskIo ReturnType, TaskIo... TaskParams>
    static auto create_task(TaskFunction<ReturnType, TaskParams...> const& task_function) -> Task;

private:
    TaskGraph m_graph;
};

}  // namespace spider::core

#endif  // SPIDER_CORE_TASKGRAPHIMPL_HPP
