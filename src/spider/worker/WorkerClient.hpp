#ifndef SPIDER_WORKER_WORKERCLIENT_HPP
#define SPIDER_WORKER_WORKERCLIENT_HPP

#include <memory>
#include <optional>
#include <string>
#include <tuple>

#include <boost/uuid/uuid.hpp>

#include "spider/io/BoostAsio.hpp"  // IWYU pragma: keep
#include "spider/storage/DataStorage.hpp"
#include "spider/storage/MetadataStorage.hpp"
#include "spider/storage/StorageFactory.hpp"

namespace spider::worker {
class WorkerClient {
public:
    // Delete copy & move constructors and assignment operators
    WorkerClient(WorkerClient const&) = delete;
    auto operator=(WorkerClient const&) -> WorkerClient& = delete;
    WorkerClient(WorkerClient&&) = delete;
    auto operator=(WorkerClient&&) -> WorkerClient& = delete;
    ~WorkerClient() = default;

    WorkerClient(
            boost::uuids::uuid worker_id,
            std::string worker_addr,
            std::shared_ptr<core::DataStorage> data_store,
            std::shared_ptr<core::MetadataStorage> metadata_store,
            std::shared_ptr<core::StorageFactory> storage_factory
    );

    auto get_next_task(std::optional<boost::uuids::uuid> const& fail_task_id)
            -> std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>>;

private:
    boost::uuids::uuid m_worker_id;
    std::string m_worker_addr;

    std::shared_ptr<core::DataStorage> m_data_store;
    std::shared_ptr<core::MetadataStorage> m_metadata_store;
    std::shared_ptr<core::StorageFactory> m_storage_factory;
};
}  // namespace spider::worker
#endif  // SPIDER_WORKER_WORKERCLIENT_HPP
