#ifndef SPIDER_WORKER_TASKEXECUTOR_HPP
#define SPIDER_WORKER_TASKEXECUTOR_HPP

#include <cstdint>
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
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/stdio.hpp>

#include "../utils/BoostAsio.hpp"  // IWYU pragma: keep
#include "../utils/MsgPack.hpp"  // IWYU pragma: keep
#include "FunctionManager.hpp"
#include "message_pipe.hpp"

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
    explicit TaskExecutor(
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
        boost::filesystem::path const exe = boost::process::v2::environment::find_executable(
                "spider_task_executor",
                environment
        );
        m_process = std::make_unique<boost::process::v2::process>(
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
        msgpack::sbuffer args_request = core::create_args_request(std::forward<Args>(args)...);
        send_message(m_write_pipe, args_request);
    }

    TaskExecutor(TaskExecutor const&) = delete;
    auto operator=(TaskExecutor const&) -> TaskExecutor& = delete;
    TaskExecutor(TaskExecutor&&) = delete;
    auto operator=(TaskExecutor&&) -> TaskExecutor& = delete;
    ~TaskExecutor() = default;

    auto completed() -> bool;
    auto waiting() -> bool;
    auto succeed() -> bool;
    auto error() -> bool;

    void wait();

    void cancel();

    template <class T>
    auto get_result() -> std::optional<T> {
        return core::response_get_result<T>(m_result_buffer);
    }

    [[nodiscard]] auto get_error() const -> std::tuple<core::FunctionInvokeError, std::string>;

private:
    auto process_output_handler() -> boost::asio::awaitable<void>;

    std::mutex m_state_mutex;
    TaskExecutorState m_state = TaskExecutorState::Running;

    // Use `std::unique_ptr` to work around requirement of default constructor
    std::unique_ptr<boost::process::v2::process> m_process = nullptr;
    boost::asio::readable_pipe m_read_pipe;
    boost::asio::writable_pipe m_write_pipe;

    msgpack::sbuffer m_result_buffer;
};

}  // namespace spider::worker

#endif  // SPIDER_WORKER_TASKEXECUTOR_HPP
