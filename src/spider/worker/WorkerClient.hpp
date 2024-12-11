#ifndef SPIDER_WORKER_WORKERCLIENT_HPP
#define SPIDER_WORKER_WORKERCLIENT_HPP

#include <future>
#include <memory>
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
            std::shared_ptr<core::DataStorage> data_store,
            std::shared_ptr<core::MetadataStorage> metadata_store
    );

    auto task_finish(
            core::TaskInstance const& instance,
            std::vector<std::variant<std::string, boost::uuids::uuid>> const& outputs
    ) -> std::future<boost::uuids::uuid>;

private:
    boost::asio::io_context m_context;

    std::shared_ptr<core::DataStorage> m_data_store;
    std::shared_ptr<core::MetadataStorage> m_metadata_store;
};
}  // namespace spider::worker
#endif  // SPIDER_WORKER_WORKERCLIENT_HPP
