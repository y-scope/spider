#ifndef SPIDER_STORAGE_METADATASTORAGE_HPP
#define SPIDER_STORAGE_METADATASTORAGE_HPP

#include <string>
#include <tuple>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Driver.hpp"
#include "../core/Error.hpp"
#include "../core/JobMetadata.hpp"
#include "../core/Task.hpp"
#include "../core/TaskGraph.hpp"

namespace spider::core {
class MetadataStorage {
public:
    MetadataStorage() = default;
    MetadataStorage(MetadataStorage const&) = delete;
    MetadataStorage(MetadataStorage&&) = delete;
    auto operator=(MetadataStorage const&) -> MetadataStorage& = delete;
    auto operator=(MetadataStorage&&) -> MetadataStorage& = delete;
    virtual ~MetadataStorage() = default;

    virtual auto initialize(StorageConnection& conn) -> StorageErr = 0;

    virtual auto add_driver(StorageConnection& conn, Driver const& driver) -> StorageErr = 0;
    virtual auto add_scheduler(StorageConnection& conn, Scheduler const& scheduler) -> StorageErr
                                                                                       = 0;
    virtual auto get_active_scheduler(StorageConnection& conn, std::vector<Scheduler>* schedulers)
            -> StorageErr = 0;

    virtual auto add_job(
            StorageConnection& conn,
            boost::uuids::uuid job_id,
            boost::uuids::uuid client_id,
            TaskGraph const& task_graph
    ) -> StorageErr = 0;
    virtual auto get_job_metadata(StorageConnection& conn, boost::uuids::uuid id, JobMetadata* job)
            -> StorageErr = 0;
    virtual auto get_job_complete(StorageConnection& conn, boost::uuids::uuid id, bool* complete)
            -> StorageErr = 0;
    virtual auto get_job_status(StorageConnection& conn, boost::uuids::uuid id, JobStatus* status)
            -> StorageErr = 0;
    virtual auto get_job_output_tasks(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::vector<boost::uuids::uuid>* task_ids
    ) -> StorageErr = 0;
    virtual auto get_task_graph(
            StorageConnection& conn,
            boost::uuids::uuid id,
            TaskGraph* task_graph
    ) -> StorageErr = 0;
    virtual auto get_jobs_by_client_id(
            StorageConnection& conn,
            boost::uuids::uuid client_id,
            std::vector<boost::uuids::uuid>* job_ids
    ) -> StorageErr = 0;
    virtual auto remove_job(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto reset_job(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto add_child(StorageConnection& conn, boost::uuids::uuid parent_id, Task const& child)
            -> StorageErr = 0;
    virtual auto get_task(StorageConnection& conn, boost::uuids::uuid id, Task* task) -> StorageErr
                                                                                         = 0;
    virtual auto get_task_job_id(
            StorageConnection& conn,
            boost::uuids::uuid id,
            boost::uuids::uuid* job_id
    ) -> StorageErr = 0;
    virtual auto get_ready_tasks(StorageConnection& conn, std::vector<Task>* tasks) -> StorageErr
                                                                                       = 0;
    virtual auto set_task_state(StorageConnection& conn, boost::uuids::uuid id, TaskState state)
            -> StorageErr = 0;
    virtual auto set_task_running(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto
    add_task_instance(StorageConnection& conn, TaskInstance const& instance) -> StorageErr = 0;
    // Set task state and add new task instance if task is ready or all instances timed out
    virtual auto
    create_task_instance(StorageConnection& conn, TaskInstance const& instance) -> StorageErr = 0;
    virtual auto task_finish(
            StorageConnection& conn,
            TaskInstance const& instance,
            std::vector<TaskOutput> const& outputs
    ) -> StorageErr = 0;
    virtual auto task_fail(
            StorageConnection& conn,
            TaskInstance const& instance,
            std::string const& error
    ) -> StorageErr = 0;
    virtual auto get_task_timeout(
            StorageConnection& conn,
            std::vector<std::tuple<TaskInstance, Task>>* tasks
    ) -> StorageErr = 0;
    virtual auto get_child_tasks(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::vector<Task>* children
    ) -> StorageErr = 0;
    virtual auto get_parent_tasks(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::vector<Task>* tasks
    ) -> StorageErr = 0;

    virtual auto update_heartbeat(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto heartbeat_timeout(
            StorageConnection& conn,
            double timeout,
            std::vector<boost::uuids::uuid>* ids
    ) -> StorageErr = 0;
    virtual auto get_scheduler_state(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::string* state
    ) -> StorageErr = 0;
    virtual auto get_scheduler_addr(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::string* addr,
            int* port
    ) -> StorageErr = 0;
    virtual auto set_scheduler_state(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::string const& state
    ) -> StorageErr = 0;
};

}  // namespace spider::core
#endif  // SPIDER_STORAGE_METADATASTORAGE_HPP
