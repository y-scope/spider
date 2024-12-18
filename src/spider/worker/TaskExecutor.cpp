#include "TaskExecutor.hpp"

#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <boost/process/v2/process.hpp>
#include <fmt/format.h>

#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "FunctionManager.hpp"
#include "message_pipe.hpp"
#include "TaskExecutorMessage.hpp"

namespace spider::worker {

auto TaskExecutor::completed() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Succeed == m_state || TaskExecutorState::Error == m_state
           || TaskExecutorState::Cancelled == m_state;
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

void TaskExecutor::wait() {
    int const exit_code = m_process->wait();
    if (exit_code != 0) {
        std::lock_guard const lock(m_state_mutex);
        if (m_state != TaskExecutorState::Cancelled && m_state != TaskExecutorState::Error) {
            m_state = TaskExecutorState::Error;
            core::create_error_buffer(
                    core::FunctionInvokeError::FunctionExecutionError,
                    fmt::format("Subprocess exit with {}", exit_code),
                    m_result_buffer
            );
        }
        return;
    }
    std::unique_lock lock(m_state_mutex);
    m_complete_cv.wait(lock, [this] {
        return TaskExecutorState::Succeed == m_state || TaskExecutorState::Error == m_state
               || TaskExecutorState::Cancelled == m_state;
    });
    lock.unlock();
}

void TaskExecutor::cancel() {
    m_process->terminate();
    std::lock_guard const lock(m_state_mutex);
    m_state = TaskExecutorState::Cancelled;
    msgpack::packer packer{m_result_buffer};
    packer.pack("Task cancelled");
}

// NOLINTBEGIN(clang-analyzer-core.CallAndMessage)
auto TaskExecutor::process_output_handler() -> boost::asio::awaitable<void> {
    while (true) {
        std::optional<msgpack::sbuffer> const response_option
                = co_await receive_message_async(m_read_pipe);
        if (!response_option.has_value()) {
            std::lock_guard const lock(m_state_mutex);
            m_state = TaskExecutorState::Error;
            core::create_error_buffer(
                    core::FunctionInvokeError::FunctionExecutionError,
                    "Pipe read fails",
                    m_result_buffer
            );
            co_return;
        }
        msgpack::sbuffer const& response = response_option.value();
        switch (get_response_type(response)) {
            case TaskExecutorResponseType::Block:
                break;
            case TaskExecutorResponseType::Error: {
                {
                    std::lock_guard const lock(m_state_mutex);
                    m_state = TaskExecutorState::Error;
                    m_result_buffer.write(response.data(), response.size());
                }
                m_complete_cv.notify_all();
                co_return;
            }
            case TaskExecutorResponseType::Ready:
                break;
            case TaskExecutorResponseType::Result: {
                {
                    std::lock_guard const lock(m_state_mutex);
                    m_state = TaskExecutorState::Succeed;
                    m_result_buffer.write(response.data(), response.size());
                }
                m_complete_cv.notify_all();
                co_return;
            }
            case TaskExecutorResponseType::Cancel: {
                {
                    std::lock_guard const lock(m_state_mutex);
                    m_state = TaskExecutorState::Cancelled;
                    m_result_buffer.write(response.data(), response.size());
                }
                m_complete_cv.notify_all();
                co_return;
            }
            case TaskExecutorResponseType::Unknown:
                break;
        }
    }
}

// NOLINTEND(clang-analyzer-core.CallAndMessage)

auto TaskExecutor::get_result_buffers() const -> std::optional<std::vector<msgpack::sbuffer>> {
    return core::response_get_result_buffers(m_result_buffer);
}

auto TaskExecutor::get_error() const -> std::tuple<core::FunctionInvokeError, std::string> {
    return core::response_get_error(m_result_buffer)
            .value_or(std::make_tuple(
                    core::FunctionInvokeError::ResultParsingError,
                    "Fail to parse error message"
            ));
}
}  // namespace spider::worker
