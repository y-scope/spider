#ifndef SPIDER_STORAGE_DATASTORAGE_HPP
#define SPIDER_STORAGE_DATASTORAGE_HPP

#include <boost/uuid/uuid.hpp>
#include <string>

#include "../core/Data.hpp"
#include "../core/Error.hpp"

namespace spider::core {
class DataStorage {
public:
    virtual StorageErr connect(std::string url, boost::uuids::uuid id) = 0;
    virtual void close() = 0;
    virtual StorageErr initialize() = 0;

    virtual StorageErr add_data(Data const& data) = 0;
    virtual StorageErr get_data(boost::uuids::uuid id, Data& data) = 0;
    virtual StorageErr add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) = 0;
    virtual StorageErr remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) = 0;
    virtual StorageErr add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
            = 0;
    virtual StorageErr remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
            = 0;
};
}  // namespace spider::core

#endif  // SPIDER_STORAGE_DATASTORAGE_HPP
