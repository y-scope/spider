#ifndef SPIDER_STORAGE_MYSQLSTORAGEFACTORY_HPP
#define SPIDER_STORAGE_MYSQLSTORAGEFACTORY_HPP

#include <memory>
#include <string>
#include <variant>

#include "spider/core/Error.hpp"
#include "spider/storage/DataStorage.hpp"
#include "spider/storage/JobSubmissionBatch.hpp"
#include "spider/storage/MetadataStorage.hpp"
#include "spider/storage/StorageConnection.hpp"
#include "spider/storage/StorageFactory.hpp"

namespace spider::core {
class MySqlStorageFactory : public StorageFactory {
public:
    explicit MySqlStorageFactory(std::string url);

    auto provide_data_storage() -> std::unique_ptr<DataStorage> override;
    auto provide_metadata_storage() -> std::unique_ptr<MetadataStorage> override;
    auto provide_storage_connection()
            -> std::variant<std::unique_ptr<StorageConnection>, StorageErr> override;
    auto provide_job_submission_batch(StorageConnection&)
            -> std::unique_ptr<JobSubmissionBatch> override;

private:
    std::string m_url;
};
}  // namespace spider::core

#endif
