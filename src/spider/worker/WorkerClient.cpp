#include "WorkerClient.hpp"

namespace spider::worker {

WorkerClient::WorkerClient(
        std::shared_ptr<core::DataStorage> data_store,
        std::shared_ptr<core::MetadataStorage> metadata_store
)
        : m_data_store(std::move(data_store)),
          m_metadata_store(std::move(metadata_store)) {}

}  // namespace spider::worker
