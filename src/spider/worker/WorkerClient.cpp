#include "WorkerClient.hpp"

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <spdlog/spdlog.h>

#include "../core/Driver.hpp"
#include "../core/Error.hpp"
#include "../core/Task.hpp"
#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/msgpack_message.hpp"
#include "../scheduler/SchedulerMessage.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "../storage/StorageFactory.hpp"

namespace spider::worker {

WorkerClient::WorkerClient(
        boost::uuids::uuid const worker_id,
        std::string worker_addr,
        std::shared_ptr<core::DataStorage> data_store,
        std::shared_ptr<core::MetadataStorage> metadata_store,
        std::shared_ptr<core::StorageFactory> storage_factory
)
        : m_worker_id{worker_id},
          m_worker_addr{std::move(worker_addr)},
          m_data_store(std::move(data_store)),
          m_metadata_store(std::move(metadata_store)),
          m_storage_factory(std::move(storage_factory)) {}

auto WorkerClient::get_next_task(std::optional<boost::uuids::uuid> const& fail_task_id
) -> std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> {
    // Get schedulers
    std::vector<core::Scheduler> schedulers;

    {  // Keep the scope for RAII storage connection
        std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                = m_storage_factory->provide_storage_connection();
        if (std::holds_alternative<core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<core::StorageErr>(conn_result).description
            );
            return std::nullopt;
        }
        auto conn = std::get<std::unique_ptr<core::StorageConnection>>(std::move(conn_result));
        if (!m_metadata_store->get_active_scheduler(*conn, &schedulers).success()) {
            return std::nullopt;
        }
    }
    if (schedulers.empty()) {
        return std::nullopt;
    }

    std::random_device random_device;
    std::default_random_engine rng{random_device()};
    std::ranges::shuffle(schedulers, rng);

    std::vector<boost::asio::ip::tcp::endpoint> endpoints;
    std::ranges::transform(
            schedulers,
            std::back_inserter(endpoints),
            [](core::Scheduler const& scheduler) {
                return boost::asio::ip::tcp::endpoint{
                        boost::asio::ip::make_address(scheduler.get_addr()),
                        static_cast<unsigned short>(scheduler.get_port())
                };
            }
    );
    try {
        // Create socket to scheduler
        boost::asio::io_context context;
        boost::asio::ip::tcp::socket socket(context);
        boost::asio::connect(socket, endpoints);

        scheduler::ScheduleTaskRequest request{m_worker_id, m_worker_addr};
        if (fail_task_id.has_value()) {
            request = scheduler::ScheduleTaskRequest{
                    m_worker_id,
                    m_worker_addr,
                    fail_task_id.value()
            };
        }
        msgpack::sbuffer request_buffer;
        msgpack::pack(request_buffer, request);

        core::send_message(socket, request_buffer);

        // Receive response
        std::optional<msgpack::sbuffer> const optional_response_buffer
                = core::receive_message(socket);
        if (!optional_response_buffer.has_value()) {
            return std::nullopt;
        }
        msgpack::sbuffer const& response_buffer = optional_response_buffer.value();

        scheduler::ScheduleTaskResponse response;
        msgpack::object_handle const response_handle
                = msgpack::unpack(response_buffer.data(), response_buffer.size());
        response_handle.get().convert(response);

        if (!response.has_task_id()) {
            return std::nullopt;
        }
        boost::uuids::uuid const task_id = response.get_task_id();

        std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                = m_storage_factory->provide_storage_connection();
        if (std::holds_alternative<core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<core::StorageErr>(conn_result).description
            );
            return std::nullopt;
        }
        auto conn = std::get<std::unique_ptr<core::StorageConnection>>(std::move(conn_result));

        core::TaskInstance const instance{task_id};
        core::StorageErr const err = m_metadata_store->create_task_instance(*conn, instance);
        if (!err.success()) {
            return std::nullopt;
        }
        return std::make_tuple(task_id, instance.id);
    } catch (boost::system::system_error const& e) {
        return std::nullopt;
    } catch (std::runtime_error const& e) {
        return std::nullopt;
    }
}

}  // namespace spider::worker
