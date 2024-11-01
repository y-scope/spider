#ifndef SPIDER_STORAGE_MYSQLSTORAGE_HPP
#define SPIDER_STORAGE_MYSQLSTORAGE_HPP

#include <boost/uuid/uuid.hpp>
#include <mariadb/conncpp.hpp>
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
    StorageErr connect(std::string const& url) override;
    void close() override;
    StorageErr initialize() override;
    StorageErr add_driver(boost::uuids::uuid id, std::string const& addr) override;
    StorageErr add_driver(boost::uuids::uuid id, std::string const& addr, int port) override;
    StorageErr add_task_graph(TaskGraph const& task_graph) override;
    StorageErr get_task_graph(boost::uuids::uuid id, TaskGraph* task_graph) override;
    StorageErr get_task_graphs(std::vector<boost::uuids::uuid>* task_graphs) override;
    StorageErr remove_task_graph(boost::uuids::uuid id) override;
    StorageErr add_child(boost::uuids::uuid parent_id, Task const& child) override;
    StorageErr get_task(boost::uuids::uuid id, Task* task) override;
    StorageErr get_ready_tasks(std::vector<Task>* tasks) override;
    StorageErr set_task_state(boost::uuids::uuid id, TaskState state) override;
    StorageErr add_task_instance(TaskInstance const& instance) override;
    StorageErr task_finish(TaskInstance const& instance) override;
    StorageErr get_task_timeout(std::vector<TaskInstance>* tasks) override;
    StorageErr get_child_task(boost::uuids::uuid id, Task* child) override;
    StorageErr get_parent_tasks(boost::uuids::uuid id, std::vector<Task>* tasks) override;
    StorageErr update_heartbeat(boost::uuids::uuid id) override;
    StorageErr heartbeat_timeout(float timeout, std::vector<boost::uuids::uuid>* ids) override;
    StorageErr get_scheduler_state(boost::uuids::uuid id, std::string* state) override;
    StorageErr set_scheduler_state(boost::uuids::uuid id, std::string const& state) override;

private:
    sql::Connection* m_conn = nullptr;

    void add_task(sql::bytes job_id, Task const& task);
    Task fetch_task(std::shared_ptr<sql::ResultSet> res);
};

class MysqlDataStorage : public DataStorage {
public:
    StorageErr connect(std::string const& url) override;
    void close() override;
    StorageErr initialize() override;
    StorageErr add_data(Data const& data) override;
    StorageErr get_data(boost::uuids::uuid id, Data* data) override;
    StorageErr add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) override;
    StorageErr remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) override;
    StorageErr add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) override;
    StorageErr
    remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) override;

private:
    sql::Connection* m_conn = nullptr;
};
}  // namespace spider::core

#endif  // SPIDER_STORAGE_MYSQLSTORAGE_HPP
