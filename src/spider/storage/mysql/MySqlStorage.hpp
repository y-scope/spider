#ifndef SPIDER_STORAGE_MYSQLSTORAGE_HPP
#define SPIDER_STORAGE_MYSQLSTORAGE_HPP

#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <mariadb/conncpp/CArray.hpp>
#include <mariadb/conncpp/ResultSet.hpp>

#include "../../core/Data.hpp"
#include "../../core/Driver.hpp"
#include "../../core/Error.hpp"
#include "../../core/JobMetadata.hpp"
#include "../../core/KeyValueData.hpp"
#include "../../core/Task.hpp"
#include "../../core/TaskGraph.hpp"
#include "../DataStorage.hpp"
#include "../MetadataStorage.hpp"
#include "../StorageConnection.hpp"
#include "MySqlConnection.hpp"

namespace spider::core {
class MySqlMetadataStorage : public MetadataStorage {
public:
    MySqlMetadataStorage() = delete;

    explicit MySqlMetadataStorage(std::string url) : m_url{std::move(url)} {}

    MySqlMetadataStorage(MySqlMetadataStorage const&) = delete;
    MySqlMetadataStorage(MySqlMetadataStorage&&) = delete;
    auto operator=(MySqlMetadataStorage const&) -> MySqlMetadataStorage& = delete;
    auto operator=(MySqlMetadataStorage&&) -> MySqlMetadataStorage& = delete;
    ~MySqlMetadataStorage() override = default;
    auto initialize(StorageConnection& conn) -> StorageErr override;
    auto add_driver(StorageConnection& conn, Driver const& driver) -> StorageErr override;
    auto add_scheduler(StorageConnection& conn, Scheduler const& scheduler) -> StorageErr override;
    auto get_active_scheduler(StorageConnection& conn, std::vector<Scheduler>* schedulers)
            -> StorageErr override;
    auto add_job(
            StorageConnection& conn,
            boost::uuids::uuid job_id,
            boost::uuids::uuid client_id,
            TaskGraph const& task_graph
    ) -> StorageErr override;
    auto add_job_batch(
            StorageConnection& conn,
            JobSubmissionBatch& batch,
            boost::uuids::uuid job_id,
            boost::uuids::uuid client_id,
            TaskGraph const& task_graph
    ) -> StorageErr override;
    auto get_job_metadata(StorageConnection& conn, boost::uuids::uuid id, JobMetadata* job)
            -> StorageErr override;
    auto get_job_complete(StorageConnection& conn, boost::uuids::uuid id, bool* complete)
            -> StorageErr override;
    auto get_job_status(StorageConnection& conn, boost::uuids::uuid id, JobStatus* status)
            -> StorageErr override;
    auto get_job_output_tasks(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::vector<boost::uuids::uuid>* task_ids
    ) -> StorageErr override;
    auto get_task_graph(StorageConnection& conn, boost::uuids::uuid id, TaskGraph* task_graph)
            -> StorageErr override;
    auto get_jobs_by_client_id(
            StorageConnection& conn,
            boost::uuids::uuid client_id,
            std::vector<boost::uuids::uuid>* job_ids
    ) -> StorageErr override;
    auto remove_job(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr override;
    auto reset_job(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr override;
    auto add_child(StorageConnection& conn, boost::uuids::uuid parent_id, Task const& child)
            -> StorageErr override;
    auto
    get_task(StorageConnection& conn, boost::uuids::uuid id, Task* task) -> StorageErr override;
    auto get_task_job_id(StorageConnection& conn, boost::uuids::uuid id, boost::uuids::uuid* job_id)
            -> StorageErr override;
    auto get_ready_tasks(StorageConnection& conn, std::vector<Task>* tasks) -> StorageErr override;
    auto set_task_state(StorageConnection& conn, boost::uuids::uuid id, TaskState state)
            -> StorageErr override;
    auto set_task_running(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr override;
    auto
    add_task_instance(StorageConnection& conn, TaskInstance const& instance) -> StorageErr override;
    auto create_task_instance(StorageConnection& conn, TaskInstance const& instance)
            -> StorageErr override;
    auto task_finish(
            StorageConnection& conn,
            TaskInstance const& instance,
            std::vector<TaskOutput> const& outputs
    ) -> StorageErr override;
    auto task_fail(StorageConnection& conn, TaskInstance const& instance, std::string const& error)
            -> StorageErr override;
    auto get_task_timeout(
            StorageConnection& conn,
            std::vector<std::tuple<TaskInstance, Task>>* tasks
    ) -> StorageErr override;
    auto get_child_tasks(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::vector<Task>* children
    ) -> StorageErr override;
    auto get_parent_tasks(StorageConnection& conn, boost::uuids::uuid id, std::vector<Task>* tasks)
            -> StorageErr override;
    auto update_heartbeat(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr override;
    auto heartbeat_timeout(
            StorageConnection& conn,
            double timeout,
            std::vector<boost::uuids::uuid>* ids
    ) -> StorageErr override;
    auto get_scheduler_state(StorageConnection& conn, boost::uuids::uuid id, std::string* state)
            -> StorageErr override;
    auto get_scheduler_addr(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::string* addr,
            int* port
    ) -> StorageErr override;
    auto set_scheduler_state(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::string const& state
    ) -> StorageErr override;

    [[nodiscard]] auto get_url() const -> std::string const& override { return m_url; }

private:
    std::string m_url;

    static void add_task(
            MySqlConnection& conn,
            sql::bytes job_id,
            Task const& task,
            std::optional<TaskState> const& state
    );
    static auto
    fetch_full_task(MySqlConnection& conn, std::unique_ptr<sql::ResultSet> const& res) -> Task;
};

class MySqlDataStorage : public DataStorage {
public:
    MySqlDataStorage() = delete;

    explicit MySqlDataStorage(std::string url) : m_url{std::move(url)} {}

    MySqlDataStorage(MySqlDataStorage const&) = delete;
    MySqlDataStorage(MySqlDataStorage&&) = delete;
    auto operator=(MySqlDataStorage const&) -> MySqlDataStorage& = delete;
    auto operator=(MySqlDataStorage&&) -> MySqlDataStorage& = delete;
    ~MySqlDataStorage() override = default;
    auto initialize(StorageConnection& conn) -> StorageErr override;
    auto add_driver_data(StorageConnection& conn, boost::uuids::uuid driver_id, Data const& data)
            -> StorageErr override;
    auto add_task_data(StorageConnection& conn, boost::uuids::uuid task_id, Data const& data)
            -> StorageErr override;
    auto
    get_data(StorageConnection& conn, boost::uuids::uuid id, Data* data) -> StorageErr override;
    auto set_data_locality(StorageConnection& conn, Data const& data) -> StorageErr override;
    auto remove_data(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr override;
    auto add_task_reference(
            StorageConnection& conn,
            boost::uuids::uuid id,
            boost::uuids::uuid task_id
    ) -> StorageErr override;
    auto remove_task_reference(
            StorageConnection& conn,
            boost::uuids::uuid id,
            boost::uuids::uuid task_id
    ) -> StorageErr override;
    auto add_driver_reference(
            StorageConnection& conn,
            boost::uuids::uuid id,
            boost::uuids::uuid driver_id
    ) -> StorageErr override;
    auto remove_driver_reference(
            StorageConnection& conn,
            boost::uuids::uuid id,
            boost::uuids::uuid driver_id
    ) -> StorageErr override;
    auto remove_dangling_data(StorageConnection& conn) -> StorageErr override;

    auto
    add_client_kv_data(StorageConnection& conn, KeyValueData const& data) -> StorageErr override;
    auto add_task_kv_data(StorageConnection& conn, KeyValueData const& data) -> StorageErr override;
    auto get_client_kv_data(
            StorageConnection& conn,
            boost::uuids::uuid const& client_id,
            std::string const& key,
            std::string* value
    ) -> StorageErr override;
    auto get_task_kv_data(
            StorageConnection& conn,
            boost::uuids::uuid const& task_id,
            std::string const& key,
            std::string* value
    ) -> StorageErr override;

    [[nodiscard]] auto get_url() const -> std::string const& override { return m_url; }

private:
    std::string m_url;
};
}  // namespace spider::core

#endif  // SPIDER_STORAGE_MYSQLSTORAGE_HPP
