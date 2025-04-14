#include "SchedulerServer.hpp"

#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <spdlog/spdlog.h>

#include "../core/Error.hpp"
#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/msgpack_message.hpp"
#include "../io/Serializer.hpp"  // IWYU pragma: keep
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "../utils/StopToken.hpp"
#include "SchedulerMessage.hpp"
#include "SchedulerPolicy.hpp"

namespace spider::scheduler {
SchedulerServer::SchedulerServer(
        unsigned short const port,
        std::shared_ptr<SchedulerPolicy> policy,
        std::shared_ptr<core::MetadataStorage> metadata_store,
        std::shared_ptr<core::DataStorage> data_store,
        std::shared_ptr<core::StorageConnection> conn,
        core::StopToken& stop_token
)
        : m_port{port},
          m_policy{std::move(policy)},
          m_metadata_store{std::move(metadata_store)},
          m_data_store{std::move(data_store)},
          m_conn{std::move(conn)} {
    boost::asio::co_spawn(m_context, receive_message(), boost::asio::detached);
    std::lock_guard const lock{m_mutex};
    m_thread = std::make_unique<std::thread>([&] { m_context.run(); });
}

auto SchedulerServer::pause() -> void {
    std::lock_guard const lock{m_mutex};
    if (m_thread == nullptr) {
        return;
    }
    m_context.stop();
    m_thread->join();
    m_thread = nullptr;
}

auto SchedulerServer::resume() -> void {
    std::lock_guard const lock{m_mutex};
    if (m_thread != nullptr) {
        return;
    }
    m_thread = std::make_unique<std::thread>([&] {
        m_context.restart();
        m_context.run();
    });
}

auto SchedulerServer::stop() -> void {
    std::lock_guard const lock{m_mutex};
    if (m_thread == nullptr) {
        return;
    }
    m_context.stop();
    m_thread->join();
    m_thread = nullptr;
}

auto SchedulerServer::receive_message() -> boost::asio::awaitable<void> {
    try {
        boost::asio::ip::tcp::acceptor acceptor{m_context, {boost::asio::ip::tcp::v4(), m_port}};
        while (true) {
            boost::asio::ip::tcp::socket socket{m_context};
            auto const& [ec] = co_await acceptor.async_accept(
                    socket,
                    boost::asio::as_tuple(boost::asio::use_awaitable)
            );
            if (ec) {
                spdlog::error("Cannot accept connection {}: {}", ec.value(), ec.what());
                continue;
            }
            boost::asio::co_spawn(
                    m_context,
                    process_message(std::move(socket)),
                    boost::asio::detached
            );
        }
        co_return;
    } catch (boost::system::system_error& e) {
        spdlog::error("Fail to accept connection: {}", e.what());
        spider::core::StopToken::request_stop();
        co_return;
    }
}

namespace {
auto deserialize_message(msgpack::sbuffer const& buffer) -> std::optional<ScheduleTaskRequest> {
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();
        return object.as<ScheduleTaskRequest>();
    } catch (std::runtime_error& e) {
        spdlog::error("Cannot unpack message to ScheduleTaskRequest: {}", e.what());
        return std::nullopt;
    }
}
}  // namespace

auto SchedulerServer::process_message(boost::asio::ip::tcp::socket socket)
        -> boost::asio::awaitable<void> {
    // NOLINTBEGIN(clang-analyzer-core.CallAndMessage)
    std::optional<msgpack::sbuffer> const& optional_message_buffer
            = co_await core::receive_message_async(socket);
    // NOLINTEND(clang-analyzer-core.CallAndMessage)

    if (false == optional_message_buffer.has_value()) {
        spdlog::error("Cannot receive message from worker");
        co_return;
    }
    msgpack::sbuffer const& message_buffer = optional_message_buffer.value();
    std::optional<ScheduleTaskRequest> const& optional_request
            = deserialize_message(message_buffer);
    if (false == optional_request.has_value()) {
        spdlog::error("Cannot parse message into schedule task request");
        co_return;
    }
    ScheduleTaskRequest const& request = optional_request.value();

    // Reset the whole job if the task fails
    if (request.has_task_id()) {
        boost::uuids::uuid job_id;
        core::StorageErr err
                = m_metadata_store->get_task_job_id(*m_conn, request.get_task_id(), &job_id);
        // It is possible the job is deleted, so we don't need to reset it
        if (!err.success()) {
            spdlog::error(
                    "Cannot get job id for task {}",
                    boost::uuids::to_string(request.get_task_id())
            );
        } else {
            err = m_metadata_store->reset_job(*m_conn, job_id);
            if (!err.success()) {
                spdlog::error("Cannot reset job {}", boost::uuids::to_string(job_id));
                co_return;
            }
        }
    }

    std::optional<boost::uuids::uuid> const task_id
            = m_policy->schedule_next(request.get_worker_id(), request.get_worker_addr());
    ScheduleTaskResponse response{};
    if (task_id.has_value()) {
        response = ScheduleTaskResponse{task_id.value()};
    }
    msgpack::sbuffer response_buffer;
    msgpack::pack(response_buffer, response);

    bool const success = co_await core::send_message_async(socket, response_buffer);
    if (!success) {
        spdlog::error(
                "Cannot send message to worker {} at {}",
                boost::uuids::to_string(request.get_worker_id()),
                request.get_worker_addr()
        );
    }
    co_return;
}
}  // namespace spider::scheduler
