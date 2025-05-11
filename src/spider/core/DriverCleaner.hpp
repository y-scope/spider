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
 * Keep tracks of the number of exceptions and clean up the driver if the
 * `Driver` holding `unique_ptr` to this class is destructed.
 *
 * We cannot use custom destructor directly inside `Driver` in case of moved
 * `Driver` object being destructed, so we work around by using a `unique_ptr`
 * so the destructor is not called by moved object.
 */
class DriverCleaner {
public:
    DriverCleaner(
            boost::uuids::uuid driver_id,
            std::shared_ptr<MetadataStorage> metadata_store,
            std::shared_ptr<StorageFactory> storage_factory,
            std::shared_ptr<StorageConnection> connection
    );

    ~DriverCleaner();

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
