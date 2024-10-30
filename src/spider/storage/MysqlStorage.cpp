#include "MysqlStorage.hpp"

#include <boost/uuid/uuid.hpp>
#include <string>
#include <vector>

#include "../core/Data.hpp"
#include "../core/Error.hpp"
#include "../core/Task.hpp"
#include "../core/TaskGraph.hpp"

namespace spider::core {
auto MySqlMetadataStorage::connect(std::string /*url*/, boost::uuids::uuid /*id*/) -> StorageErr {
    return StorageErr{};
}

void MySqlMetadataStorage::close() {}

auto MySqlMetadataStorage::initialize() -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::add_task_graph(TaskGraph const& /*task_graph*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_graph(boost::uuids::uuid /*id*/, TaskGraph& /*task_graph*/)
        -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_graphs(std::vector<TaskGraph>& /*task_graphs*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::remove_task_graph(boost::uuids::uuid /*id*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::add_child(boost::uuids::uuid /*parent_id*/, Task const& /*child*/)
        -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task(boost::uuids::uuid /*id*/, Task& /*task*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::get_ready_tasks(std::vector<Task>& /*tasks*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::set_task_state(boost::uuids::uuid /*id*/, TaskState /*state*/)
        -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::add_task_instance(TaskInstance const& /*instance*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::task_finish(TaskInstance const& /*instance*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_timeout(std::vector<Task>& /*tasks*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::get_child_task(boost::uuids::uuid /*id*/, Task& /*child*/)
        -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::get_parent_tasks(boost::uuids::uuid /*id*/, std::vector<Task>& /*tasks*/)
        -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::update_heartbeat(boost::uuids::uuid /*id*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlMetadataStorage::heartbeat_timeout(std::vector<boost::uuids::uuid>& /*ids*/)
        -> StorageErr {
    return StorageErr{};
}

auto MySqlDataStorage::connect(std::string /*url*/, boost::uuids::uuid /*id*/) -> StorageErr {
    return StorageErr{};
}

void MySqlDataStorage::close() {}

auto MySqlDataStorage::initialize() -> StorageErr {
    return StorageErr{};
}

auto MySqlDataStorage::add_data(Data const& /*data*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlDataStorage::get_data(boost::uuids::uuid /*id*/, Data& /*data*/) -> StorageErr {
    return StorageErr{};
}

auto MySqlDataStorage::add_task_reference(boost::uuids::uuid /*id*/, boost::uuids::uuid /*task_id*/)
        -> StorageErr {
    return StorageErr{};
}

auto MySqlDataStorage::
        remove_task_reference(boost::uuids::uuid /*id*/, boost::uuids::uuid /*task_id*/)
                -> StorageErr {
    return StorageErr{};
}

auto MySqlDataStorage::
        add_driver_reference(boost::uuids::uuid /*id*/, boost::uuids::uuid /*driver_id*/)
                -> StorageErr {
    return StorageErr{};
}

auto MySqlDataStorage::remove_driver_reference(
        boost::uuids::uuid /*id*/,
        boost::uuids::uuid /*driver_id*/
) -> StorageErr {
    return StorageErr{};
}

}  // namespace spider::core
