#ifndef SPIDER_CORE_DATACLEANER_HPP
#define SPIDER_CORE_DATACLEANER_HPP

#include <exception>
#include <memory>

#include <boost/uuid/uuid.hpp>

#include "../storage/DataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "../storage/StorageFactory.hpp"
#include "Context.hpp"

namespace spider::core {
/*
 * Tracks the number of exceptions and ensures proper cleanup when the `Data`
 * object—holding a `unique_ptr` to this class—is destroyed.
 *
 * We avoid placing a custom destructor directly inside `Data` because, when a
 * `Data` object is moved, its destructor is still invoked. This can result in
 * double deletion. By using a `unique_ptr`, we ensure that destruction only
 * occurs for the final owner, preventing such issues.
 *
 * We use std::uncaught_exceptions() to track the number of exceptions so that
 * we don't clean up if the destructor is called during exception handling.
 */
class DataCleaner {
public:
    DataCleaner(
            boost::uuids::uuid data_id,
            Context const& context,
            std::shared_ptr<DataStorage> data_storage,
            std::shared_ptr<StorageFactory> storage_factory,
            std::shared_ptr<StorageConnection> storage_connection
    );
    ~DataCleaner() noexcept;

    // Delete copy constructor and assignment operator
    DataCleaner(DataCleaner const&) = delete;
    auto operator=(DataCleaner const&) -> DataCleaner& = delete;
    // Default move constructor and assignment operator
    DataCleaner(DataCleaner&&) = default;
    auto operator=(DataCleaner&&) -> DataCleaner& = default;

private:
    int m_num_exceptions = std::uncaught_exceptions();

    boost::uuids::uuid m_data_id;
    Context m_context;
    std::shared_ptr<DataStorage> m_data_store;
    std::shared_ptr<StorageFactory> m_storage_factory;
    std::shared_ptr<StorageConnection> m_connection = nullptr;
};
}  // namespace spider::core

#endif
