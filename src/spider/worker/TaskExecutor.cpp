#include "TaskExecutor.hpp"

#include <unistd.h>

#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <fmt/format.h>

#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/worker/FunctionManager.hpp>
#include <spider/worker/message_pipe.hpp>
#include <spider/worker/TaskExecutorMessage.hpp>

namespace spider::worker {
auto TaskExecutor::get_task_id() const -> boost::uuids::uuid {
    return m_task_id;
}

auto TaskExecutor::get_pid() const -> pid_t {
    return m_process->get_pid();
}

auto TaskExecutor::completed() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Succeed == m_state || TaskExecutorState::Error == m_state
           || TaskExecutorState::Cancelled == m_state;
}

auto TaskExecutor::waiting() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Waiting == m_state;
}

auto TaskExecutor::succeeded() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Succeed == m_state;
}

auto TaskExecutor::errored() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Error == m_state;
}

auto TaskExecutor::cancelled() -> bool {
    std::lock_guard const lock(m_state_mutex);
    return TaskExecutorState::Cancelled == m_state;
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
        {
            std::lock_guard const lock(m_state_mutex);
            if (m_state != TaskExecutorState::Waiting && m_state != TaskExecutorState::Running) {
                co_return;
            }
        }
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
            .value_or(
                    std::make_tuple(
                            core::FunctionInvokeError::ResultParsingError,
                            "Fail to parse error message"
                    )
            );
}
}  // namespace spider::worker
