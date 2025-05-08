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
 * Keep tracks of the number of exceptions and clean up the data if the `Data`
 * holding `unique_ptr` to this class is destructed.
 *
 * We cannot use custom destructor directly inside `Data` in case of moved
 * `Data` object being destructed, so we work around by using a `unique_ptr`
 * so the destructor is not called by moved object.
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
    ~DataCleaner();

    // Default copy constructor and assignment operator
    DataCleaner(DataCleaner const&) = default;
    auto operator=(DataCleaner const&) -> DataCleaner& = default;
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
