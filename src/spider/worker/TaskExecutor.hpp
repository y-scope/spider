#ifndef SPIDER_WORKER_TASKEXECUTOR_HPP
#define SPIDER_WORKER_TASKEXECUTOR_HPP

#include <unistd.h>

#include <array>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/filesystem/path.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "FunctionManager.hpp"
#include "message_pipe.hpp"
#include "Process.hpp"

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
    template <class... Args>
    TaskExecutor(
            boost::asio::io_context& context,
            std::string const& func_name,
            boost::uuids::uuid const task_id,
            std::string const& storage_url,
            std::vector<std::string> const& libs,
            absl::flat_hash_map<
                    boost::process::v2::environment::key,
                    boost::process::v2::environment::value> const& environment,
            Args&&... args
    )
            : m_read_pipe(context),
              m_write_pipe(context) {
        std::vector<std::string> process_args{
                "--func",
                func_name,
                "--task_id",
                to_string(task_id),
                "--storage_url",
                storage_url,
                "--libs"
        };
        process_args.insert(process_args.end(), libs.begin(), libs.end());
        boost::filesystem::path const exe = boost::process::v2::environment::find_executable(
                "spider_task_executor",
                environment
        );
        std::array<int, 2> write_pipe_fd{};
        std::array<int, 2> read_pipe_fd{};
        if (pipe(read_pipe_fd.data()) == -1 || pipe(write_pipe_fd.data()) == -1) {
            throw std::runtime_error("Failed to create pipe");
        }
        m_write_pipe.assign(write_pipe_fd[1]);
        m_read_pipe.assign(read_pipe_fd[0]);
        m_process = std::make_unique<Process>(Process::spawn(
                exe.string(),
                process_args,
                write_pipe_fd[0],
                read_pipe_fd[1],
                std::nullopt
        ));
        close(write_pipe_fd[0]);
        close(read_pipe_fd[1]);

        // Set up handler for output file
        boost::asio::co_spawn(context, process_output_handler(), boost::asio::detached);

        // Send args
        msgpack::sbuffer const args_request
                = core::create_args_request(std::forward<Args>(args)...);
        send_message(m_write_pipe, args_request);
    }

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
        std::vector<std::string> process_args{
                "--func",
                func_name,
                "--task_id",
                to_string(task_id),
                "--storage_url",
                storage_url,
                "--libs"
        };
        process_args.insert(process_args.end(), libs.begin(), libs.end());
        boost::filesystem::path const exe = boost::process::v2::environment::find_executable(
                "spider_task_executor",
                environment
        );
        std::array<int, 2> write_pipe_fd{};
        std::array<int, 2> read_pipe_fd{};
        if (pipe(read_pipe_fd.data()) == -1 || pipe(write_pipe_fd.data()) == -1) {
            throw std::runtime_error("Failed to create pipe");
        }
        m_write_pipe.assign(write_pipe_fd[1]);
        m_read_pipe.assign(read_pipe_fd[0]);
        m_process = std::make_unique<Process>(Process::spawn(
                exe.string(),
                process_args,
                write_pipe_fd[0],
                read_pipe_fd[1],
                std::nullopt
        ));
        close(write_pipe_fd[0]);
        close(read_pipe_fd[1]);

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

    auto is_completed() -> bool;
    auto is_waiting() -> bool;
    auto is_succeeded() -> bool;
    auto is_error() -> bool;
    auto is_cancelled() -> bool;

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
