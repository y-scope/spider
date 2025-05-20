#ifndef SPIDER_STORAGE_STORAGEFACTORY_HPP
#define SPIDER_STORAGE_STORAGEFACTORY_HPP

#include <memory>
#include <variant>

#include "spider/core/Error.hpp"
#include "spider/storage/DataStorage.hpp"
#include "spider/storage/JobSubmissionBatch.hpp"
#include "spider/storage/MetadataStorage.hpp"
#include "spider/storage/StorageConnection.hpp"

namespace spider::core {
class StorageFactory {
public:
    virtual auto provide_data_storage() -> std::unique_ptr<DataStorage> = 0;
    virtual auto provide_metadata_storage() -> std::unique_ptr<MetadataStorage> = 0;
    virtual auto provide_storage_connection()
            -> std::variant<std::unique_ptr<StorageConnection>, StorageErr>
            = 0;
    virtual auto provide_job_submission_batch(StorageConnection&)
            -> std::unique_ptr<JobSubmissionBatch>
            = 0;

    StorageFactory() = default;
    StorageFactory(StorageFactory const&) = default;
    auto operator=(StorageFactory const&) -> StorageFactory& = default;
    StorageFactory(StorageFactory&&) = default;
    auto operator=(StorageFactory&&) -> StorageFactory& = default;
    virtual ~StorageFactory() = default;
};
}  // namespace spider::core

#endif
