#ifndef SPIDER_STORAGE_MYSQLSTORAGE_HPP
#define SPIDER_STORAGE_MYSQLSTORAGE_HPP

#include <memory>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <mariadb/conncpp/CArray.hpp>
#include <mariadb/conncpp/Connection.hpp>
#include <mariadb/conncpp/ResultSet.hpp>

#include "../client/Job.hpp"
#include "../core/Data.hpp"
#include "../core/Driver.hpp"
#include "../core/Error.hpp"
#include "../core/JobMetadata.hpp"
#include "../core/KeyValueData.hpp"
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
    auto connect(std::string const& url) -> StorageErr override;
    void close() override;
    auto initialize() -> StorageErr override;
    auto add_driver(Driver const& driver) -> StorageErr override;
    auto add_scheduler(Scheduler const& scheduler) -> StorageErr override;
    auto get_driver(boost::uuids::uuid id, std::string* addr) -> StorageErr override;
    auto get_active_scheduler(std::vector<Scheduler>* schedulers) -> StorageErr override;
    auto
    add_job(boost::uuids::uuid job_id, boost::uuids::uuid client_id, TaskGraph const& task_graph
    ) -> StorageErr override;
    auto get_job_metadata(boost::uuids::uuid id, JobMetadata* job) -> StorageErr override;
    auto get_job_complete(boost::uuids::uuid id, bool* complete) -> StorageErr override;
    auto get_job_status(boost::uuids::uuid id, JobStatus* status) -> StorageErr override;
    auto get_job_output_tasks(boost::uuids::uuid id, std::vector<boost::uuids::uuid>* task_ids)
            -> StorageErr override;
    auto get_task_graph(boost::uuids::uuid id, TaskGraph* task_graph) -> StorageErr override;
    auto get_jobs_by_client_id(
            boost::uuids::uuid client_id,
            std::vector<boost::uuids::uuid>* job_ids
    ) -> StorageErr override;
    auto remove_job(boost::uuids::uuid id) -> StorageErr override;
    auto reset_job(boost::uuids::uuid id) -> StorageErr override;
    auto add_child(boost::uuids::uuid parent_id, Task const& child) -> StorageErr override;
    auto get_task(boost::uuids::uuid id, Task* task) -> StorageErr override;
    auto get_task_job_id(boost::uuids::uuid id, boost::uuids::uuid* job_id) -> StorageErr override;
    auto get_ready_tasks(std::vector<Task>* tasks) -> StorageErr override;
    auto set_task_state(boost::uuids::uuid id, TaskState state) -> StorageErr override;
    auto add_task_instance(TaskInstance const& instance) -> StorageErr override;
    auto task_finish(TaskInstance const& instance, std::vector<TaskOutput> const& outputs)
            -> StorageErr override;
    auto task_fail(TaskInstance const& instance, std::string const& error) -> StorageErr override;
    auto get_task_timeout(std::vector<TaskInstance>* tasks) -> StorageErr override;
    auto get_child_tasks(boost::uuids::uuid id, std::vector<Task>* children) -> StorageErr override;
    auto get_parent_tasks(boost::uuids::uuid id, std::vector<Task>* tasks) -> StorageErr override;
    auto update_heartbeat(boost::uuids::uuid id) -> StorageErr override;
    auto
    heartbeat_timeout(double timeout, std::vector<boost::uuids::uuid>* ids) -> StorageErr override;
    auto get_scheduler_state(boost::uuids::uuid id, std::string* state) -> StorageErr override;
    auto
    get_scheduler_addr(boost::uuids::uuid id, std::string* addr, int* port) -> StorageErr override;
    auto
    set_scheduler_state(boost::uuids::uuid id, std::string const& state) -> StorageErr override;

private:
    sql::Connection* m_conn = nullptr;

    void add_task(sql::bytes job_id, Task const& task);
    auto fetch_full_task(std::unique_ptr<sql::ResultSet> const& res) -> Task;
};

class MySqlDataStorage : public DataStorage {
public:
    MySqlDataStorage() = default;
    MySqlDataStorage(MySqlDataStorage const&) = delete;
    MySqlDataStorage(MySqlDataStorage&&) = delete;
    auto operator=(MySqlDataStorage const&) -> MySqlDataStorage& = delete;
    auto operator=(MySqlDataStorage&&) -> MySqlDataStorage& = delete;
    ~MySqlDataStorage() override = default;
    auto connect(std::string const& url) -> StorageErr override;
    void close() override;
    auto initialize() -> StorageErr override;
    auto add_driver_data(boost::uuids::uuid driver_id, Data const& data) -> StorageErr override;
    auto add_task_data(boost::uuids::uuid task_id, Data const& data) -> StorageErr override;
    auto get_data(boost::uuids::uuid id, Data* data) -> StorageErr override;
    auto set_data_locality(Data const& data) -> StorageErr override;
    auto remove_data(boost::uuids::uuid id) -> StorageErr override;
    auto
    add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) -> StorageErr override;
    auto
    remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) -> StorageErr override;
    auto add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
            -> StorageErr override;
    auto remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
            -> StorageErr override;
    auto remove_dangling_data() -> StorageErr override;

    auto add_client_kv_data(KeyValueData const& data) -> StorageErr override;
    auto add_task_kv_data(KeyValueData const& data) -> StorageErr override;
    auto get_client_kv_data(
            boost::uuids::uuid const& client_id,
            std::string const& key,
            std::string* value
    ) -> StorageErr override;
    auto get_task_kv_data(
            boost::uuids::uuid const& task_id,
            std::string const& key,
            std::string* value
    ) -> StorageErr override;

private:
    sql::Connection* m_conn = nullptr;
};
}  // namespace spider::core

#endif  // SPIDER_STORAGE_MYSQLSTORAGE_HPP
