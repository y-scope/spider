#include "TaskExecutor.hpp"

#include "FunctionManager.hpp"

namespace spider::worker {
template <class... Args>
TaskExecutor::TaskExecutor(std::string const& /*func_name*/, Args&&... /*args*/) {}

auto TaskExecutor::completed() -> bool {}

auto TaskExecutor::succeed() -> bool {}

auto TaskExecutor::error() -> bool {}

auto TaskExecutor::wait() {}

template <class T>
auto TaskExecutor::get_result() -> T {}

auto TaskExecutor::get_error() -> std::tuple<core::FunctionInvokeError, std::string> {}
}  // namespace spider::worker
