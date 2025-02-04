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
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Driver.hpp"
#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/msgpack_message.hpp"
#include "../scheduler/SchedulerMessage.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"

namespace spider::worker {

WorkerClient::WorkerClient(
        boost::uuids::uuid const worker_id,
        std::string worker_addr,
        std::shared_ptr<core::DataStorage> data_store,
        std::shared_ptr<core::MetadataStorage> metadata_store
)
        : m_worker_id{worker_id},
          m_worker_addr{std::move(worker_addr)},
          m_data_store(std::move(data_store)),
          m_metadata_store(std::move(metadata_store)) {}

auto WorkerClient::get_next_task(std::optional<boost::uuids::uuid> const& fail_task_id
) -> std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> {
    // Get schedulers
    std::vector<core::Scheduler> schedulers;
    if (!m_metadata_store->get_active_scheduler(&schedulers).success()) {
        return std::nullopt;
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
        return response.get_task_ids();
    } catch (boost::system::system_error const& e) {
        return std::nullopt;
    } catch (std::runtime_error const& e) {
        return std::nullopt;
    }
}

}  // namespace spider::worker
