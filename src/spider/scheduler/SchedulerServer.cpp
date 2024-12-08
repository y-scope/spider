#include "SchedulerServer.hpp"

#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <spdlog/spdlog.h>

#include "../core/Task.hpp"
#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/msgpack_message.hpp"
#include "../io/Serializer.hpp"  // IWYU pragma: keep
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "SchedulerMessage.hpp"
#include "SchedulerPolicy.hpp"

namespace spider::scheduler {

SchedulerServer::SchedulerServer(
        unsigned short const port,
        std::shared_ptr<SchedulerPolicy> policy,
        std::shared_ptr<core::MetadataStorage> metadata_store,
        std::shared_ptr<core::DataStorage> data_store
)
        : m_acceptor{m_context, {boost::asio::ip::tcp::v4(), port}},
          m_policy{std::move(policy)},
          m_metadata_store{std::move(metadata_store)},
          m_data_store{std::move(data_store)} {
    // Ignore the returned future as we do not need its value
    boost::asio::co_spawn(m_context, receive_message(), boost::asio::use_future);
}

auto SchedulerServer::receive_message() -> boost::asio::awaitable<void> {
    while (!should_stop()) {
        // std::unique_ptr<boost::asio::ip::tcp::socket> socket
        //         = std::make_unique<boost::asio::ip::tcp::socket>(m_context);
        boost::asio::ip::tcp::socket socket{m_context};
        auto const& [ec] = co_await m_acceptor.async_accept(
                socket,
                boost::asio::as_tuple(boost::asio::use_awaitable)
        );
        if (ec) {
            spdlog::error("Cannot accept connection {}: {}", ec.value(), ec.what());
        }
        // Ignore the returned future as we do not need its value
        boost::asio::co_spawn(
                m_context,
                process_message(std::move(socket)),
                boost::asio::use_future
        );
    }
    co_return;
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

auto SchedulerServer::process_message(boost::asio::ip::tcp::socket socket
) -> boost::asio::awaitable<void> {
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

    if (request.is_task_complete()) {
        submit_task(request.get_task_id(), request.get_task_instance_id());
    }

    std::optional<boost::uuids::uuid> const task_id = m_policy->schedule_next(
            m_metadata_store,
            m_data_store,
            request.get_worker_id(),
            request.get_worker_addr()
    );
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

auto SchedulerServer::submit_task(
        boost::uuids::uuid const task_id,
        boost::uuids::uuid const task_instance_id
) -> void {
    core::TaskInstance const instance{task_instance_id, task_id};
    m_metadata_store->task_finish(instance);
}

auto SchedulerServer::should_stop() -> bool {
    std::lock_guard const lock{m_mutex};
    return m_stop;
}

}  // namespace spider::scheduler
