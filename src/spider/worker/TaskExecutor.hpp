#ifndef SPIDER_WORKER_TASKEXECUTOR_HPP
#define SPIDER_WORKER_TASKEXECUTOR_HPP

#include <unistd.h>

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/filesystem/path.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/utils/pipe.hpp>
#include <spider/worker/FunctionManager.hpp>
#include <spider/worker/message_pipe.hpp>
#include <spider/worker/Process.hpp>

namespace spider::worker {
enum class TaskExecutorState : std::uint8_t {
    Running,
    Waiting,
    Succeed,
    Error,
    Cancelled,
};

class TaskExecutor {
public:
    TaskExecutor(
            boost::asio::io_context& context,
            std::string const& func_name,
            boost::uuids::uuid const task_id,
            std::string const& storage_url,
            std::vector<std::string> const& libs,
            absl::flat_hash_map<
                    boost::process::v2::environment::key,
                    boost::process::v2::environment::value> const& environment,
            std::vector<msgpack::sbuffer> const& args_buffers
    )
            : m_read_pipe(context),
              m_write_pipe(context) {
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
                "--libs"
        };
        process_args.insert(process_args.end(), libs.begin(), libs.end());
        boost::filesystem::path const exe = boost::process::v2::environment::find_executable(
                "spider_task_executor",
                environment
        );

        m_write_pipe.assign(input_pipe_write_end);
        m_read_pipe.assign(output_pipe_read_end);
        m_process = std::make_unique<Process>(Process::spawn(
                exe.string(),
                process_args,
                std::nullopt,
                std::nullopt,
                std::nullopt,
                {input_pipe_read_end, output_pipe_write_end}
        ));
        // Close the following fds since they're no longer needed by the parent process.
        close(input_pipe_read_end);
        close(output_pipe_write_end);

        // Set up handler for output file
        boost::asio::co_spawn(context, process_output_handler(), boost::asio::detached);

        // Send args
        msgpack::sbuffer const args_request = core::create_args_request(args_buffers);
        send_message(m_write_pipe, args_request);
    }

    TaskExecutor(TaskExecutor const&) = delete;
    auto operator=(TaskExecutor const&) -> TaskExecutor& = delete;
    TaskExecutor(TaskExecutor&&) = delete;
    auto operator=(TaskExecutor&&) -> TaskExecutor& = delete;
    ~TaskExecutor() = default;

    /*
     * @return The process ID of the task executor.
     */
    [[nodiscard]] auto get_pid() const -> pid_t;

    auto completed() -> bool;
    auto waiting() -> bool;
    auto succeed() -> bool;
    auto error() -> bool;

    void wait();

    void cancel();

    template <class T>
    auto get_result() const -> std::optional<T> {
        return core::response_get_result<T>(m_result_buffer);
    }

    [[nodiscard]] auto get_result_buffers() const -> std::optional<std::vector<msgpack::sbuffer>>;

    [[nodiscard]] auto get_error() const -> std::tuple<core::FunctionInvokeError, std::string>;

private:
    auto process_output_handler() -> boost::asio::awaitable<void>;

    std::mutex m_state_mutex;
    std::condition_variable m_complete_cv;
    TaskExecutorState m_state = TaskExecutorState::Running;

    // Use `std::unique_ptr` to work around requirement of default constructor
    std::unique_ptr<Process> m_process = nullptr;
    boost::asio::readable_pipe m_read_pipe;
    boost::asio::writable_pipe m_write_pipe;

    msgpack::sbuffer m_result_buffer;
};
}  // namespace spider::worker

#endif  // SPIDER_WORKER_TASKEXECUTOR_HPP
