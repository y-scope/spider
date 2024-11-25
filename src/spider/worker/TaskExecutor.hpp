#ifndef SPIDER_WORKER_TASKEXECUTOR_HPP
#define SPIDER_WORKER_TASKEXECUTOR_HPP

#include <cstdint>
#include <string>
#include <tuple>

#include "../core/MsgPack.hpp"  // IWYU pragma: keep
#include "FunctionManager.hpp"

namespace spider::worker {

enum class TaskExecutorState : std::uint8_t {
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
    msgpack::sbuffer m_args_buffer;
    TaskExecutorState m_state = TaskExecutorState::Running;
};

}  // namespace spider::worker

#endif  // SPIDER_WORKER_TASKEXECUTOR_HPP
