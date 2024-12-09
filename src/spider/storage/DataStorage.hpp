#ifndef SPIDER_STORAGE_DATASTORAGE_HPP
#define SPIDER_STORAGE_DATASTORAGE_HPP

#include <string>

#include <boost/uuid/uuid.hpp>

#include "../core/Data.hpp"
#include "../core/Error.hpp"
#include "../core/KeyValueData.hpp"

namespace spider::core {
class DataStorage {
public:
    DataStorage() = default;
    DataStorage(DataStorage const&) = delete;
    DataStorage(DataStorage&&) = delete;
    auto operator=(DataStorage const&) -> DataStorage& = delete;
    auto operator=(DataStorage&&) -> DataStorage& = delete;
    virtual ~DataStorage() = default;

    virtual auto connect(std::string const& url) -> StorageErr = 0;
    virtual void close() = 0;
    virtual auto initialize() -> StorageErr = 0;

    virtual auto add_driver_data(boost::uuids::uuid driver_id, Data const& data) -> StorageErr = 0;
    virtual auto add_task_data(boost::uuids::uuid task_id, Data const& data) -> StorageErr = 0;
    virtual auto get_data(boost::uuids::uuid id, Data* data) -> StorageErr = 0;
    virtual auto remove_data(boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) -> StorageErr
                                                                                          = 0;
    virtual auto
    remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) -> StorageErr = 0;
    virtual auto
    add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) -> StorageErr = 0;
    virtual auto
    remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) -> StorageErr = 0;
    virtual auto remove_dangling_data() -> StorageErr = 0;

    virtual auto add_client_kv_data(KeyValueData const& data) -> StorageErr = 0;
    virtual auto add_task_kv_data(KeyValueData const& data) -> StorageErr = 0;
    virtual auto get_client_kv_data(
            boost::uuids::uuid const& client_id,
            std::string const& key,
            std::string* value
    ) -> StorageErr = 0;
    virtual auto get_task_kv_data(
            boost::uuids::uuid const& task_id,
            std::string const& key,
            std::string* value
    ) -> StorageErr = 0;
};
}  // namespace spider::core

#endif  // SPIDER_STORAGE_DATASTORAGE_HPP
