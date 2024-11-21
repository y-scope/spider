#ifndef SPIDER_WORKER_TASKEXECUTOR_HPP
#define SPIDER_WORKER_TASKEXECUTOR_HPP

#include <boost/process/child.hpp>

#include "FunctionManager.hpp"
#include "../core/MsgPack.hpp" // IWYU pragma: keep

namespace spider::worker {

enum class TaskExecutorState {
    Running,
    Succeed,
    Error,
};

class TaskExecutor {
public:
    template <class... Args>
    explicit TaskExecutor(std::string const& func_name, Args&&... args);

    auto completed() -> bool;
    auto succeed() -> bool;
    auto error() -> bool;

    auto wait();

    template <class T>
    auto get_result() -> T;

    auto get_error() -> std::tuple<core::FunctionInvokeError, std::string>;

private:
    boost::process::child m_process;
    msgpack::sbuffer m_args_buffer;
    TaskExecutorState m_state = TaskExecutorState::Running;

};


}

#endif // SPIDER_WORKER_TASKEXECUTOR_HPP


