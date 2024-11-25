#include "TaskExecutor.hpp"

#include <string>
#include <tuple>

#include "FunctionManager.hpp"

namespace spider::worker {
template <class... Args>
TaskExecutor::TaskExecutor(std::string const& /*func_name*/, Args&&... /*args*/) {}

auto TaskExecutor::completed() -> bool {
    return TaskExecutorState::Running == m_state;
}

auto TaskExecutor::succeed() -> bool {
    return TaskExecutorState::Succeed == m_state;
}

auto TaskExecutor::error() -> bool {
    return TaskExecutorState::Error == m_state;
}

auto TaskExecutor::wait() {}

template <class T>
auto TaskExecutor::get_result() -> T {
    return T{};
}

auto TaskExecutor::get_error() -> std::tuple<core::FunctionInvokeError, std::string> {
    return std::make_tuple(spider::core::FunctionInvokeError::Success, "");
}
}  // namespace spider::worker
