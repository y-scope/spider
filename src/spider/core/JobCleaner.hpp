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
 * Keep tracks of the number of exceptions and clean up the job if the `Job`
 * holding `unique_ptr` to this class is destructed.
 *
 * We cannot use custom destructor directly inside `Job` in case of moved
 * `Job` object being destructed, so we work around by using a `unique_ptr`
 * so the destructor is not called by moved object.
 */
class JobCleaner {
public:
    JobCleaner(
            boost::uuids::uuid job_id,
            std::shared_ptr<MetadataStorage> metadata_store,
            std::shared_ptr<StorageFactory> storage_factory,
            std::shared_ptr<StorageConnection> connection
    );

    ~JobCleaner();

    // Default copy constructor and assignment operator
    JobCleaner(JobCleaner const&) = default;
    auto operator=(JobCleaner const&) -> JobCleaner& = default;
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
