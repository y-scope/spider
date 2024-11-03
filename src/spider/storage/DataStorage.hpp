#ifndef SPIDER_STORAGE_DATASTORAGE_HPP
#define SPIDER_STORAGE_DATASTORAGE_HPP

#include <boost/uuid/uuid.hpp>
#include <string>

#include "../core/Data.hpp"
#include "../core/Error.hpp"

namespace spider::core {
class DataStorage {
public:
    DataStorage() = default;
    DataStorage(DataStorage const&) = delete;
    DataStorage(DataStorage&&) = delete;
    auto operator=(DataStorage const&) -> DataStorage& = delete;
    auto operator=(DataStorage&&) -> DataStorage& = delete;
    virtual ~DataStorage() = default;

    virtual auto connect(std::string url, boost::uuids::uuid id) -> StorageErr = 0;
    virtual void close() = 0;
    virtual auto initialize() -> StorageErr = 0;

    virtual auto add_data(Data const& data) -> StorageErr = 0;
    virtual auto get_data(boost::uuids::uuid id, Data& data) -> StorageErr = 0;
    virtual auto add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) -> StorageErr
                                                                                          = 0;
    virtual auto
    remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) -> StorageErr = 0;
    virtual auto
    add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) -> StorageErr = 0;
    virtual auto
    remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) -> StorageErr = 0;
};
}  // namespace spider::core

#endif  // SPIDER_STORAGE_DATASTORAGE_HPP
