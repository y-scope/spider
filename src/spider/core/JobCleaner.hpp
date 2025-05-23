#ifndef SPIDER_CORE_JOB_CLEANER_HPP
#define SPIDER_CORE_JOB_CLEANER_HPP

#include <exception>
#include <memory>

#include <boost/uuid/uuid.hpp>

#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "../storage/StorageFactory.hpp"

namespace spider::core {
/*
 * Tracks the number of exceptions and ensures proper cleanup when the `Job`
 * object—holding a `unique_ptr` to this class—is destroyed.
 *
 * We avoid placing a custom destructor directly inside `Job` because, when a
 * `Job` object is moved, its destructor is still invoked. This can result in
 * double deletion. By using a `unique_ptr`, we ensure that destruction only
 * occurs for the final owner, preventing such issues.
 */
class JobCleaner {
public:
    JobCleaner(
            boost::uuids::uuid job_id,
            std::shared_ptr<MetadataStorage> metadata_store,
            std::shared_ptr<StorageFactory> storage_factory,
            std::shared_ptr<StorageConnection> connection
    );

    ~JobCleaner() noexcept;

    // Delete copy constructor and assignment operator
    JobCleaner(JobCleaner const&) = delete;
    auto operator=(JobCleaner const&) -> JobCleaner& = delete;
    // Default move constructor and assignment operator
    JobCleaner(JobCleaner&&) = default;
    auto operator=(JobCleaner&&) -> JobCleaner& = default;

private:
    int m_num_exceptions = std::uncaught_exceptions();

    boost::uuids::uuid m_job_id;
    std::shared_ptr<MetadataStorage> m_metadata_store;
    std::shared_ptr<StorageFactory> m_storage_factory;
    std::shared_ptr<StorageConnection> m_connection = nullptr;
};
}  // namespace spider::core

#endif
