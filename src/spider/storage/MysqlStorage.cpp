#include "MysqlStorage.hpp"

namespace spider::core {
StorageErr MySqlMetadataStorage::connect(std::string url, boost::uuids::uuid id) {}

void MySqlMetadataStorage::close() {}

StorageErr MySqlMetadataStorage::initialize() {}

StorageErr MySqlMetadataStorage::add_task_graph(TaskGraph const& task_graph) {}

StorageErr MySqlMetadataStorage::get_task_graph(boost::uuids::uuid id, TaskGraph& task_graph) {}

StorageErr MySqlMetadataStorage::get_task_graphs(std::vector<TaskGraph>& task_graphs) {}

StorageErr MySqlMetadataStorage::remove_task_graph(boost::uuids::uuid id) {}

StorageErr MySqlMetadataStorage::add_child(boost::uuids::uuid parent_id, Task const& child) {}

StorageErr MySqlMetadataStorage::get_task(boost::uuids::uuid id, Task& task) {}

StorageErr MySqlMetadataStorage::get_ready_tasks(std::vector<Task>& tasks) {}

StorageErr MySqlMetadataStorage::set_task_state(boost::uuids::uuid id, TaskState state) {}

StorageErr MySqlMetadataStorage::add_task_instance(TaskInstance const& instance) {}

StorageErr MySqlMetadataStorage::task_finish(TaskInstance const& instance) {}

StorageErr MySqlMetadataStorage::get_task_timeout(std::vector<Task>& tasks) {}

StorageErr get_child_task(boost::uuids::uuid id, Task& child) {}

StorageErr get_parent_tasks(boost::uuids::uuid id, std::vector<Task>& tasks) {}

StorageErr MySqlMetadataStorage::update_heartbeat(boost::uuids::uuid id) {}

StorageErr MySqlMetadataStorage::hearbeat_timeout(std::vector<boost::uuids::uuid>& ids) {}

StorageErr MysqlDataStorage::connect(std::string url, boost::uuids::uuid id) {}

void MysqlDataStorage::close() {}

StorageErr MysqlDataStorage::initialize() {}

StorageErr MysqlDataStorage::add_data(Data const& data) {}

StorageErr MysqlDataStorage::get_data(boost::uuids::uuid id, Data& data) {}

StorageErr MysqlDataStorage::add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) {
}

StorageErr
MysqlDataStorage::remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) {}

StorageErr
MysqlDataStorage::add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) {}

StorageErr
MysqlDataStorage::remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) {}

}  // namespace spider::core
