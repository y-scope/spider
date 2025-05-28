#ifndef SPIDER_CORE_DRIVERCLEANER_HPP
#define SPIDER_CORE_DRIVERCLEANER_HPP

#include <exception>
#include <memory>

#include <boost/uuid/uuid.hpp>

#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "../storage/StorageFactory.hpp"

namespace spider::core {
/*
 * Tracks the number of exceptions and ensures proper cleanup when the `Driver`
 * object—holding a `unique_ptr` to this class—is destroyed.
 *
 * We avoid placing a custom destructor directly inside `Driver` because, when a
 * `Driver` object is moved, its destructor is still invoked. This can result in
 * double deletion. By using a `unique_ptr`, we ensure that destruction only
 * occurs for the final owner, preventing such issues.
 *
 * We use std::uncaught_exceptions() to track the number of exceptions so that
 * we don't clean up if the destructor is called during exception handling.
 */
class DriverCleaner {
public:
    DriverCleaner(
            boost::uuids::uuid driver_id,
            std::shared_ptr<MetadataStorage> metadata_store,
            std::shared_ptr<StorageFactory> storage_factory,
            std::shared_ptr<StorageConnection> connection
    );

    ~DriverCleaner() noexcept;

    // Delete copy constructor and assignment operator
    DriverCleaner(DriverCleaner const&) = delete;
    auto operator=(DriverCleaner const&) -> DriverCleaner& = delete;
    // Default move constructor and assignment operator
    DriverCleaner(DriverCleaner&&) = default;
    auto operator=(DriverCleaner&&) -> DriverCleaner& = default;

private:
    int m_num_exceptions = std::uncaught_exceptions();

    boost::uuids::uuid m_driver_id;
    std::shared_ptr<MetadataStorage> m_metadata_store;
    std::shared_ptr<StorageFactory> m_storage_factory;
    std::shared_ptr<StorageConnection> m_connection = nullptr;
};
}  // namespace spider::core

#endif
