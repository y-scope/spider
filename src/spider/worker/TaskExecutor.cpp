#include "TaskExecutor.hpp"

#include <boost/process/child.hpp>
#include <boost/process/search_path.hpp>

#include "FunctionManager.hpp"

namespace spider::worker {
template<class ... Args>
TaskExecutor::TaskExecutor(std::string const& func_name, Args&&... args) {
    m_process = boost::process::child(boost::process::search_path("spider_task_executor"), {"--func", func_name}, );
    m_args_buffer = core::create_args_buffers(args);
}

auto TaskExecutor::completed() -> bool {
}

auto TaskExecutor::succeed() -> bool {
}

auto TaskExecutor::error() -> bool {
}

auto TaskExecutor::wait() {
}

template<class T>
auto TaskExecutor::get_result() -> T {
}

auto TaskExecutor::get_error() -> std::tuple<core::FunctionInvokeError, std::string> {
}
}
