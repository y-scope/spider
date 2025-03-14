#include "MySqlStorageFactory.hpp"

#include <memory>
#include <string>

#include "../../core/Error.hpp"
#include "../DataStorage.hpp"
#include "../JobSubmissionBatch.hpp"
#include "../MetadataStorage.hpp"
#include "../StorageConnection.hpp"
#include "MySqlConnection.hpp"
#include "MySqlJobSubmissionBatch.hpp"
#include "MySqlStorage.hpp"

namespace spider::core {

MySqlStorageFactory::MySqlStorageFactory(std::string const& url) : m_url{url} {}

auto MySqlStorageFactory::provide_data_storage() -> std::unique_ptr<DataStorage> {
    return std::make_unique<MySqlDataStorage>();
}

auto MySqlStorageFactory::provide_metadata_storage() -> std::unique_ptr<MetadataStorage> {
    return std::make_unique<MySqlMetadataStorage>();
}

auto MySqlStorageFactory::provide_storage_connection(
) -> std::variant<std::unique_ptr<StorageConnection>, StorageErr> {
    std::variant<MySqlConnection, StorageErr> connection = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(connection)) {
        return std::get<StorageErr>(connection);
    }
    return std::make_unique<StorageConnection>(std::move(std::get<MySqlConnection>(connection)));
}

auto MySqlStorageFactory::provide_job_submission_batch(StorageConnection& connection
) -> std::unique_ptr<JobSubmissionBatch> {
    return std::make_unique<MySqlJobSubmissionBatch>(connection);
}
}  // namespace spider::core
