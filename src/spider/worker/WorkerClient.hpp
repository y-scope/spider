#ifndef SPIDER_WORKER_WORKERCLIENT_HPP
#define SPIDER_WORKER_WORKERCLIENT_HPP

#include <memory>
#include <optional>
#include <string>
#include <variant>

#include <boost/uuid/uuid.hpp>

#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"

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
            std::shared_ptr<core::MetadataStorage> metadata_store
    );

    auto task_finish(
            core::TaskInstance const& instance,
            std::vector<std::variant<std::string, boost::uuids::uuid>> const& outputs
    ) -> std::optional<boost::uuids::uuid>;

private:
    boost::uuids::uuid m_worker_id;
    std::string m_worker_addr;

    boost::asio::io_context m_context;

    std::shared_ptr<core::DataStorage> m_data_store;
    std::shared_ptr<core::MetadataStorage> m_metadata_store;
};
}  // namespace spider::worker
#endif  // SPIDER_WORKER_WORKERCLIENT_HPP
