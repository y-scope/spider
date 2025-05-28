#include "DataCleaner.hpp"

#include <exception>
#include <memory>
#include <utility>
#include <variant>

#include <boost/uuid/uuid.hpp>

#include <spider/core/Context.hpp>
#include <spider/core/Error.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>

namespace spider::core {
DataCleaner::DataCleaner(
        boost::uuids::uuid data_id,
        Context const& context,
        std::shared_ptr<DataStorage> data_storage,
        std::shared_ptr<StorageFactory> storage_factory,
        std::shared_ptr<StorageConnection> storage_connection
)
        : m_data_id{data_id},
          m_context{context},
          m_data_store{std::move(data_storage)},
          m_storage_factory{std::move(storage_factory)},
          m_connection{std::move(storage_connection)} {}

DataCleaner::~DataCleaner() noexcept {
    int const num_exceptions = std::uncaught_exceptions();
    // If destructor is called during stack unwinding, do not remove data reference.
    if (num_exceptions > m_num_exceptions) {
        return;
    }
    std::shared_ptr<StorageConnection> conn = m_connection;
    if (nullptr == conn) {
        std::variant<std::unique_ptr<StorageConnection>, StorageErr> conn_result
                = m_storage_factory->provide_storage_connection();
        // If we cannot get the connection, that means we are in a worker.
        // Just let the reference stays until the job is removed.
        if (std::holds_alternative<StorageErr>(conn_result)) {
            return;
        }
        conn = std::move(std::get<std::unique_ptr<StorageConnection>>(conn_result));
    }
    if (m_context.get_source() == Context::Source::Driver) {
        m_data_store->remove_driver_reference(*conn, m_data_id, m_context.get_id());
    } else {
        m_data_store->remove_task_reference(*conn, m_data_id, m_context.get_id());
    }
}
}  // namespace spider::core
