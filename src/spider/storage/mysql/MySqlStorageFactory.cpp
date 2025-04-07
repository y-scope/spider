#include "MySqlStorageFactory.hpp"

#include <memory>
#include <string>
#include <utility>
#include <variant>

#include "../../core/Error.hpp"
#include "../DataStorage.hpp"
#include "../JobSubmissionBatch.hpp"
#include "../MetadataStorage.hpp"
#include "../StorageConnection.hpp"
#include "MySqlConnection.hpp"
#include "MySqlJobSubmissionBatch.hpp"
#include "MySqlStorage.hpp"

namespace spider::core {
MySqlStorageFactory::MySqlStorageFactory(std::string url) : m_url{std::move(url)} {}

auto MySqlStorageFactory::provide_data_storage() -> std::unique_ptr<DataStorage> {
    return std::unique_ptr<DataStorage>(new MySqlDataStorage());
}

auto MySqlStorageFactory::provide_metadata_storage() -> std::unique_ptr<MetadataStorage> {
    return std::unique_ptr<MetadataStorage>(new MySqlMetadataStorage());
}

auto MySqlStorageFactory::provide_storage_connection()
        -> std::variant<std::unique_ptr<StorageConnection>, StorageErr> {
    std::variant<std::unique_ptr<StorageConnection>, StorageErr> connection
            = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(connection)) {
        return std::get<StorageErr>(connection);
    }
    return std::move(std::get<std::unique_ptr<StorageConnection>>(connection));
}

auto MySqlStorageFactory::provide_job_submission_batch(StorageConnection& connection)
        -> std::unique_ptr<JobSubmissionBatch> {
    return std::unique_ptr<JobSubmissionBatch>(new MySqlJobSubmissionBatch(connection));
}
}  // namespace spider::core
