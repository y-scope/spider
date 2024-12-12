#include "WorkerClient.hpp"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Task.hpp"
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
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

auto WorkerClient::task_finish(
        core::TaskInstance const& instance,
        std::vector<core::TaskOutput> const& outputs
) -> std::optional<boost::uuids::uuid> {
    m_metadata_store->task_finish(instance, outputs);

    scheduler::ScheduleTaskRequest const request{m_worker_id, m_worker_addr};
    msgpack::sbuffer request_buffer;
    msgpack::pack(request_buffer, request);
    return std::nullopt;
}

}  // namespace spider::worker
