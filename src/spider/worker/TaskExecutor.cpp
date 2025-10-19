#include "TaskExecutor.hpp"

#include <unistd.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/filesystem/path.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>

#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/utils/pipe.hpp>
#include <spider/worker/FunctionManager.hpp>
#include <spider/worker/message_pipe.hpp>
#include <spider/worker/Process.hpp>
#include <spider/worker/TaskExecutorMessage.hpp>

namespace spider::worker {
auto TaskExecutor::spawn_cpp_executor(
        boost::asio::io_context& context,
        std::string const& func_name,
        boost::uuids::uuid const task_id,
        std::string const& storage_url,
        std::vector<std::string> const& libs,
        absl::flat_hash_map<
                boost::process::v2::environment::key,
                boost::process::v2::environment::value
        > const& environment,
        std::vector<msgpack::sbuffer> const& args_buffers
) -> std::unique_ptr<TaskExecutor> {
    auto const exe
            = boost::process::v2::environment::find_executable("spider_task_executor", environment);
    if (exe.empty()) {
        return nullptr;
    }

    auto const [input_pipe_read_end, input_pipe_write_end] = core::create_pipe();
    auto const [output_pipe_read_end, output_pipe_write_end] = core::create_pipe();

    std::vector<std::string> process_args{
            "--func",
            func_name,
            "--task_id",
            to_string(task_id),
            "--input-pipe",
            std::to_string(input_pipe_read_end),
            "--output-pipe",
            std::to_string(output_pipe_write_end),
            "--storage_url",
            storage_url,
    };
    if (false == libs.empty()) {
        process_args.emplace_back("--libs");
        process_args.insert(process_args.end(), libs.cbegin(), libs.cend());
    }

    auto executor = std::unique_ptr<TaskExecutor>(new TaskExecutor(
            context,
            output_pipe_read_end,
            input_pipe_write_end,
            std::make_unique<Process>(Process::spawn(
                    exe.string(),
                    process_args,
                    std::nullopt,
                    std::nullopt,
                    std::nullopt,
                    {input_pipe_read_end, output_pipe_write_end}
            ))
    ));
    // Close the following fds since they're no longer needed by the parent process.
    close(input_pipe_read_end);
    close(output_pipe_write_end);

    // Set up handler for output file
    boost::asio::co_spawn(context, executor->process_output_handler(), boost::asio::detached);

    // Send args
    auto const args_request = core::create_args_request(args_buffers);
    send_message(executor->m_write_pipe, args_request);

    return executor;
}

auto TaskExecutor::spawn_python_executor(
        boost::asio::io_context& context,
        std::string const& func_name,
        boost::uuids::uuid const task_id,
        std::string const& storage_url,
        absl::flat_hash_map<
                boost::process::v2::environment::key,
                boost::process::v2::environment::value
        > const& environment,
        std::vector<msgpack::sbuffer> const& args_buffers
) -> std::unique_ptr<TaskExecutor> {
    auto const exe = boost::process::v2::environment::find_executable("python3", environment);
    if (exe.empty()) {
        return nullptr;
    }

    auto const [input_pipe_read_end, input_pipe_write_end] = core::create_pipe();
    auto const [output_pipe_read_end, output_pipe_write_end] = core::create_pipe();

    std::vector<std::string> const process_args{
            "-m",
            "spider_py.task_executor.task_executor",
            "--func",
            func_name,
            "--task_id",
            to_string(task_id),
            "--input-pipe",
            std::to_string(input_pipe_read_end),
            "--output-pipe",
            std::to_string(output_pipe_write_end),
            "--storage_url",
            storage_url,
    };

    auto executor = std::unique_ptr<TaskExecutor>(new TaskExecutor(
            context,
            output_pipe_read_end,
            input_pipe_write_end,
            std::make_unique<Process>(Process::spawn(
                    exe.string(),
                    process_args,
                    std::nullopt,
                    std::nullopt,
                    std::nullopt,
                    {input_pipe_read_end, output_pipe_write_end}
            ))
    ));

    // Close the following fds since they're no longer needed by the parent process.
    close(input_pipe_read_end);
    close(output_pipe_write_end);

    // Set up handler for output file
    boost::asio::co_spawn(context, executor->process_output_handler(), boost::asio::detached);

    // Send args
    auto const args_request = core::create_args_request(args_buffers);
    send_message(executor->m_write_pipe, args_request);

    return executor;
}

TaskExecutor::TaskExecutor(
        boost::asio::io_context& context,
        int const read_pipe_fd,
        int const write_pipe_fd,
        std::unique_ptr<Process> process
)
        : m_read_pipe(context),
          m_write_pipe(context),
          m_process(std::move(process)) {
    m_read_pipe.assign(read_pipe_fd);
    m_write_pipe.assign(write_pipe_fd);
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
            .value_or(
                    std::make_tuple(
                            core::FunctionInvokeError::ResultParsingError,
                            "Fail to parse error message"
                    )
            );
}
}  // namespace spider::worker
