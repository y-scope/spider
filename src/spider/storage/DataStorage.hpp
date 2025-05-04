#ifndef SPIDER_STORAGE_DATASTORAGE_HPP
#define SPIDER_STORAGE_DATASTORAGE_HPP

#include <string>

#include <boost/uuid/uuid.hpp>

#include "../core/Data.hpp"
#include "../core/Error.hpp"
#include "../core/KeyValueData.hpp"
#include "StorageConnection.hpp"

namespace spider::core {
class DataStorage {
public:
    DataStorage() = default;
    DataStorage(DataStorage const&) = default;
    auto operator=(DataStorage const&) -> DataStorage& = default;
    DataStorage(DataStorage&&) = default;
    auto operator=(DataStorage&&) -> DataStorage& = default;
    virtual ~DataStorage() = default;

    virtual auto initialize(StorageConnection& conn) -> StorageErr = 0;

    virtual auto
    add_driver_data(StorageConnection& conn, boost::uuids::uuid driver_id, Data const& data)
            -> StorageErr
            = 0;
    virtual auto
    add_task_data(StorageConnection& conn, boost::uuids::uuid task_id, Data const& data)
            -> StorageErr
            = 0;
    virtual auto get_data(StorageConnection& conn, boost::uuids::uuid id, Data* data) -> StorageErr
            = 0;
    /**
     * Get data and register data reference from a driver in a single transaction.
     * @param conn
     * @param driver_id
     * @param data_id
     * @param data output data
     * @return StorageErr::Success if the transaction succeed. Error types otherwise.
     */
    virtual auto get_data_driver(
            StorageConnection& conn,
            boost::uuids::uuid driver_id,
            boost::uuids::uuid data_id,
            Data* data
    ) -> StorageErr
            = 0;
    /**
     * Get data and register data reference from a task in a single transaction.
     * @param conn
     * @param task_id
     * @param data_id
     * @param data output data
     * @return StorageErr::Success if the transaction succeed. Error types otherwise.
     */
    virtual auto get_data_task(
            StorageConnection& conn,
            boost::uuids::uuid task_id,
            boost::uuids::uuid data_id,
            Data* data
    ) -> StorageErr
            = 0;
    virtual auto set_data_locality(StorageConnection& conn, Data const& data) -> StorageErr = 0;
    virtual auto remove_data(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto
    add_task_reference(StorageConnection& conn, boost::uuids::uuid id, boost::uuids::uuid task_id)
            -> StorageErr
            = 0;
    virtual auto remove_task_reference(
            StorageConnection& conn,
            boost::uuids::uuid id,
            boost::uuids::uuid task_id
    ) -> StorageErr
            = 0;
    virtual auto add_driver_reference(
            StorageConnection& conn,
            boost::uuids::uuid id,
            boost::uuids::uuid driver_id
    ) -> StorageErr
            = 0;
    virtual auto remove_driver_reference(
            StorageConnection& conn,
            boost::uuids::uuid id,
            boost::uuids::uuid driver_id
    ) -> StorageErr
            = 0;
    virtual auto remove_dangling_data(StorageConnection& conn) -> StorageErr = 0;

    virtual auto add_client_kv_data(StorageConnection& conn, KeyValueData const& data) -> StorageErr
            = 0;
    virtual auto add_task_kv_data(StorageConnection& conn, KeyValueData const& data) -> StorageErr
            = 0;
    virtual auto get_client_kv_data(
            StorageConnection& conn,
            boost::uuids::uuid const& client_id,
            std::string const& key,
            std::string* value
    ) -> StorageErr
            = 0;
    virtual auto get_task_kv_data(
            StorageConnection& conn,
            boost::uuids::uuid const& task_id,
            std::string const& key,
            std::string* value
    ) -> StorageErr
            = 0;
};
}  // namespace spider::core

#endif  // SPIDER_STORAGE_DATASTORAGE_HPP
