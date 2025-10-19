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
#include <boost/process/v2/environment.hpp>
#include <boost/uuid/uuid.hpp>

#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/worker/FunctionManager.hpp>
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
    [[nodiscard]] static auto spawn_cpp_executor(
            boost::asio::io_context& context,
            std::string const& func_name,
            boost::uuids::uuid task_id,
            std::string const& storage_url,
            std::vector<std::string> const& libs,
            absl::flat_hash_map<
                    boost::process::v2::environment::key,
                    boost::process::v2::environment::value
            > const& environment,
            std::vector<msgpack::sbuffer> const& args_buffers
    ) -> std::unique_ptr<TaskExecutor>;

    static auto spawn_python_executor(
            boost::asio::io_context& context,
            std::string const& func_name,
            boost::uuids::uuid task_id,
            std::string const& storage_url,
            absl::flat_hash_map<
                    boost::process::v2::environment::key,
                    boost::process::v2::environment::value
            > const& environment,
            std::vector<msgpack::sbuffer> const& args_buffers
    ) -> std::unique_ptr<TaskExecutor>;

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
    // Constructors
    explicit TaskExecutor(
            boost::asio::io_context& context,
            int read_pipe_fd,
            int write_pipe_fd,
            std::unique_ptr<Process> process
    );

    auto process_output_handler() -> boost::asio::awaitable<void>;

    std::mutex m_state_mutex;
    std::condition_variable m_complete_cv;
    TaskExecutorState m_state = TaskExecutorState::Running;

    // Use `std::unique_ptr` to work around requirement of default constructor
    std::unique_ptr<Process> m_process;
    boost::asio::readable_pipe m_read_pipe;
    boost::asio::writable_pipe m_write_pipe;

    msgpack::sbuffer m_result_buffer;
};
}  // namespace spider::worker

#endif  // SPIDER_WORKER_TASKEXECUTOR_HPP
