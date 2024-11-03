#ifndef SPIDER_STORAGE_MYSQLSTORAGE_HPP
#define SPIDER_STORAGE_MYSQLSTORAGE_HPP

#include <boost/uuid/uuid.hpp>
#include <string>
#include <vector>

#include "../core/Data.hpp"
#include "../core/Error.hpp"
#include "../core/Task.hpp"
#include "../core/TaskGraph.hpp"
#include "DataStorage.hpp"
#include "MetadataStorage.hpp"

namespace spider::core {
class MySqlMetadataStorage : public MetadataStorage {
public:
    MySqlMetadataStorage() = default;
    MySqlMetadataStorage(MySqlMetadataStorage const&) = delete;
    MySqlMetadataStorage(MySqlMetadataStorage&&) = delete;
    auto operator=(MySqlMetadataStorage const&) -> MySqlMetadataStorage& = delete;
    auto operator=(MySqlMetadataStorage&&) -> MySqlMetadataStorage& = delete;
    ~MySqlMetadataStorage() override = default;
    auto connect(std::string url, boost::uuids::uuid id) -> StorageErr override;
    void close() override;
    auto initialize() -> StorageErr override;
    auto add_task_graph(TaskGraph const& task_graph) -> StorageErr override;
    auto get_task_graph(boost::uuids::uuid id, TaskGraph& task_graph) -> StorageErr override;
    auto get_task_graphs(std::vector<TaskGraph>& task_graphs) -> StorageErr override;
    auto remove_task_graph(boost::uuids::uuid id) -> StorageErr override;
    auto add_child(boost::uuids::uuid parent_id, Task const& child) -> StorageErr override;
    auto get_task(boost::uuids::uuid id, Task& task) -> StorageErr override;
    auto get_ready_tasks(std::vector<Task>& tasks) -> StorageErr override;
    auto set_task_state(boost::uuids::uuid id, TaskState state) -> StorageErr override;
    auto add_task_instance(TaskInstance const& instance) -> StorageErr override;
    auto task_finish(TaskInstance const& instance) -> StorageErr override;
    auto get_task_timeout(std::vector<Task>& tasks) -> StorageErr override;
    auto get_child_task(boost::uuids::uuid id, Task& child) -> StorageErr override;
    auto get_parent_tasks(boost::uuids::uuid id, std::vector<Task>& tasks) -> StorageErr override;
    auto update_heartbeat(boost::uuids::uuid id) -> StorageErr override;
    auto heartbeat_timeout(std::vector<boost::uuids::uuid>& ids) -> StorageErr override;
};

class MySqlDataStorage : public DataStorage {
public:
    MySqlDataStorage() = default;
    MySqlDataStorage(MySqlDataStorage const&) = delete;
    MySqlDataStorage(MySqlDataStorage&&) = delete;
    auto operator=(MySqlDataStorage const&) -> MySqlDataStorage& = delete;
    auto operator=(MySqlDataStorage&&) -> MySqlDataStorage& = delete;
    ~MySqlDataStorage() override = default;
    auto connect(std::string url, boost::uuids::uuid id) -> StorageErr override;
    void close() override;
    auto initialize() -> StorageErr override;
    auto add_data(Data const& data) -> StorageErr override;
    auto get_data(boost::uuids::uuid id, Data& data) -> StorageErr override;
    auto
    add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) -> StorageErr override;
    auto
    remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) -> StorageErr override;
    auto add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
            -> StorageErr override;
    auto remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
            -> StorageErr override;
};
}  // namespace spider::core

#endif  // SPIDER_STORAGE_MYSQLSTORAGE_HPP
