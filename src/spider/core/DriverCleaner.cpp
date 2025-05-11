#include "DriverCleaner.hpp"

#include <exception>
#include <memory>
#include <utility>
#include <variant>

#include <boost/uuid/uuid.hpp>

#include "../core/Error.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "../storage/StorageFactory.hpp"

namespace spider::core {
DriverCleaner::DriverCleaner(
        boost::uuids::uuid driver_id,
        std::shared_ptr<MetadataStorage> metadata_store,
        std::shared_ptr<StorageFactory> storage_factory,
        std::shared_ptr<StorageConnection> connection
)
        : m_driver_id{driver_id},
          m_metadata_store{std::move(metadata_store)},
          m_storage_factory{std::move(storage_factory)},
          m_connection{std::move(connection)} {}

DriverCleaner::~DriverCleaner() noexcept {
    int const num_exceptions = std::uncaught_exceptions();
    // If destructor is called during stack unwinding, do not remove data reference.
    if (num_exceptions > m_num_exceptions) {
        return;
    }
    std::shared_ptr<StorageConnection> conn = m_connection;
    if (nullptr == conn) {
        std::variant<std::unique_ptr<StorageConnection>, StorageErr> conn_result
                = m_storage_factory->provide_storage_connection();
        // If we cannot get the connection, just wait for the heartbeat to fail
        // and remove the driver.
        if (std::holds_alternative<StorageErr>(conn_result)) {
            return;
        }
        conn = std::move(std::get<std::unique_ptr<StorageConnection>>(conn_result));
    }
    m_metadata_store->remove_driver(*conn, m_driver_id);
}
}  // namespace spider::core
