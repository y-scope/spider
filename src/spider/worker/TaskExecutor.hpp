#ifndef SPIDER_WORKER_TASKEXECUTOR_HPP
#define SPIDER_WORKER_TASKEXECUTOR_HPP

#include <cstdint>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/process/v2/environment.hpp>
#include <boost/process/v2/process.hpp>

#include "../core/BoostAsio.hpp"  // IWYU pragma: keep
#include "../core/MsgPack.hpp"  // IWYU pragma: keep
#include "FunctionManager.hpp"

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
    );

    auto completed() -> bool;
    auto waiting() -> bool;
    auto succeed() -> bool;
    auto error() -> bool;

    auto wait();

    auto cancel();

    template <class T>
    auto get_result() -> T;

    [[nodiscard]] auto get_error() const -> std::tuple<core::FunctionInvokeError, std::string>;

private:
    auto process_output_handler() -> boost::asio::awaitable<void>;

    std::mutex m_state_mutex;
    TaskExecutorState m_state = TaskExecutorState::Running;

    boost::process::v2::process m_process;
    boost::asio::readable_pipe m_read_pipe;
    boost::asio::writable_pipe m_write_pipe;

    msgpack::sbuffer m_result_buffer;
};

}  // namespace spider::worker

#endif  // SPIDER_WORKER_TASKEXECUTOR_HPP
