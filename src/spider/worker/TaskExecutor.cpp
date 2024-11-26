#include "TaskExecutor.hpp"

#include <string>
#include <tuple>

#include <boost/filesystem/path.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/stdio.hpp>

#include "FunctionManager.hpp"
#include "message_pipe.hpp"

namespace spider::worker {

template <class... Args>
TaskExecutor::TaskExecutor(
        boost::asio::io_context& context,
        std::string const& func_name,
        std::vector<std::string> const& libs,
        absl::flat_hash_map<
                boost::process::v2::environment::key,
                boost::process::v2::environment::value> const& environment,
        Args&&... args
)
        : m_read_pipe(context),
          m_write_pipe(context) {
    std::vector<std::string> process_args{"--func", func_name, "--libs"};
    process_args.insert(process_args.end(), libs.begin(), libs.end());
    boost::filesystem::path const exe
            = boost::process::v2::environment::find_executable("task_executor", environment);
    m_process = boost::process::v2::process(
            context,
            exe,
            process_args,
            boost::process::v2::process_stdio{
                    .in = m_write_pipe,
                    .out = m_read_pipe,
                    .err = {/*stderr to default*/}
            },
            boost::process::v2::process_environment{environment}
    );

    // Set up handler for output file
    boost::asio::co_spawn(context, process_output_handler(), boost::asio::detached);

    // Send args
    msgpack::sbuffer args_request = core::create_args_request(args...);
    send_message(m_write_pipe, args_request);
}

auto TaskExecutor::completed() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Succeed == m_state || TaskExecutorState::Error == m_state;
}

auto TaskExecutor::waiting() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Waiting == m_state;
}

auto TaskExecutor::succeed() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Succeed == m_state;
}

auto TaskExecutor::error() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Error == m_state;
}

auto TaskExecutor::wait() {
    m_process.wait();
}

auto TaskExecutor::process_output_handler() -> boost::asio::awaitable<void> {
    while (true) {
        std::optional<msgpack::sbuffer> const response_option
                = co_await receive_message_async(m_read_pipe);
        if (!response_option.has_value()) {
            break;
        }
        msgpack::sbuffer const& response = response_option.value();
        switch (get_response_type(response)) {
            case TaskExecutorResponseType::Block:
                break;
            case TaskExecutorResponseType::Error: {
                std::lock_guard const lock(m_state_mutex);
                m_state = TaskExecutorState::Error;
                m_result_buffer.write(response.data(), response.size());
                break;
            }
            case TaskExecutorResponseType::Ready:
                break;
            case TaskExecutorResponseType::Result: {
                std::lock_guard const lock(m_state_mutex);
                m_state = TaskExecutorState::Succeed;
                m_result_buffer.write(response.data(), response.size());
                break;
            }
            case TaskExecutorResponseType::Unknown:
                break;
        }
    }
}

template <class T>
auto TaskExecutor::get_result() -> T {
    return core::response_get_result<T>(m_result_buffer);
}

auto TaskExecutor::get_error() const -> std::tuple<core::FunctionInvokeError, std::string> {
    return core::response_get_error(m_result_buffer).value();
}
}  // namespace spider::worker
