#ifndef SPIDER_STORAGE_METADATASTORAGE_HPP
#define SPIDER_STORAGE_METADATASTORAGE_HPP

#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include <spider/core/Driver.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/JobMetadata.hpp>
#include <spider/core/Task.hpp>
#include <spider/core/TaskGraph.hpp>
#include <spider/storage/JobSubmissionBatch.hpp>
#include <spider/storage/StorageConnection.hpp>

namespace spider::core {
class MetadataStorage {
public:
    MetadataStorage() = default;
    MetadataStorage(MetadataStorage const&) = default;
    auto operator=(MetadataStorage const&) -> MetadataStorage& = default;
    MetadataStorage(MetadataStorage&&) = default;
    auto operator=(MetadataStorage&&) -> MetadataStorage& = default;
    virtual ~MetadataStorage() = default;

    virtual auto initialize(StorageConnection& conn) -> StorageErr = 0;

    virtual auto add_driver(StorageConnection& conn, Driver const& driver) -> StorageErr = 0;
    virtual auto add_scheduler(StorageConnection& conn, Scheduler const& scheduler) -> StorageErr
            = 0;
    virtual auto get_active_scheduler(StorageConnection& conn, std::vector<Scheduler>* schedulers)
            -> StorageErr
            = 0;

    virtual auto
    add_job(StorageConnection& conn,
            boost::uuids::uuid job_id,
            boost::uuids::uuid client_id,
            TaskGraph const& task_graph) -> StorageErr
            = 0;
    virtual auto add_job_batch(
            StorageConnection& conn,
            JobSubmissionBatch& batch,
            boost::uuids::uuid job_id,
            boost::uuids::uuid client_id,
            TaskGraph const& task_graph
    ) -> StorageErr
            = 0;
    virtual auto get_job_metadata(StorageConnection& conn, boost::uuids::uuid id, JobMetadata* job)
            -> StorageErr
            = 0;
    virtual auto get_job_complete(StorageConnection& conn, boost::uuids::uuid id, bool* complete)
            -> StorageErr
            = 0;
    virtual auto get_job_status(StorageConnection& conn, boost::uuids::uuid id, JobStatus* status)
            -> StorageErr
            = 0;
    virtual auto get_job_output_tasks(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::vector<boost::uuids::uuid>* task_ids
    ) -> StorageErr
            = 0;
    virtual auto
    get_task_graph(StorageConnection& conn, boost::uuids::uuid id, TaskGraph* task_graph)
            -> StorageErr
            = 0;
    virtual auto get_jobs_by_client_id(
            StorageConnection& conn,
            boost::uuids::uuid client_id,
            std::vector<boost::uuids::uuid>* job_ids
    ) -> StorageErr
            = 0;
    /**
     * Cancel a job. This will set the job state to CANCEL and set all tasks that have not
     * finished or started to CANCEL.
     * @param conn
     * @param id The job id.
     * @return The error code.
     */
    virtual auto cancel_job(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    /**
     * Cancel a job that owns the task. This will set the job state to CANCEL and set all tasks
     * that have not finished or started to CANCEL.
     * @param conn
     * @param id The task id.
     * @param message The error message of the cancellation.
     * @return The error code.
     */
    virtual auto
    cancel_job_by_task(StorageConnection& conn, boost::uuids::uuid id, std::string const& message)
            -> StorageErr
            = 0;
    /**
     * Get the error message of a cancelled job.
     * @param conn
     * @param id The job id.
     * @param function_name The function name of the cancelled task.
     * @param message The error message of the cancellation.
     * @return The error code.
     */
    virtual auto get_job_message(
            StorageConnection& conn,
            boost::uuids::uuid id,
            std::string* function_name,
            std::string* message
    ) -> StorageErr
            = 0;
    virtual auto remove_job(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto reset_job(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto add_child(StorageConnection& conn, boost::uuids::uuid parent_id, Task const& child)
            -> StorageErr
            = 0;
    virtual auto get_task(StorageConnection& conn, boost::uuids::uuid id, Task* task) -> StorageErr
            = 0;
    virtual auto
    get_task_job_id(StorageConnection& conn, boost::uuids::uuid id, boost::uuids::uuid* job_id)
            -> StorageErr
            = 0;
    virtual auto get_ready_tasks(
            StorageConnection& conn,
            boost::uuids::uuid scheduler_id,
            std::vector<ScheduleTaskMetadata>* tasks
    ) -> StorageErr
            = 0;
    virtual auto set_task_state(StorageConnection& conn, boost::uuids::uuid id, TaskState state)
            -> StorageErr
            = 0;
    virtual auto get_task_state(StorageConnection& conn, boost::uuids::uuid id, TaskState* state)
            -> StorageErr
            = 0;
    virtual auto set_task_running(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto add_task_instance(StorageConnection& conn, TaskInstance const& instance)
            -> StorageErr
            = 0;
    // Set task state and add new task instance if task is ready or all instances timed out
    virtual auto create_task_instance(StorageConnection& conn, TaskInstance const& instance)
            -> StorageErr
            = 0;
    virtual auto task_finish(
            StorageConnection& conn,
            TaskInstance const& instance,
            std::vector<TaskOutput> const& outputs
    ) -> StorageErr
            = 0;
    virtual auto
    task_fail(StorageConnection& conn, TaskInstance const& instance, std::string const& error)
            -> StorageErr
            = 0;
    virtual auto get_task_timeout(StorageConnection& conn, std::vector<ScheduleTaskMetadata>* tasks)
            -> StorageErr
            = 0;
    virtual auto
    get_child_tasks(StorageConnection& conn, boost::uuids::uuid id, std::vector<Task>* children)
            -> StorageErr
            = 0;
    virtual auto
    get_parent_tasks(StorageConnection& conn, boost::uuids::uuid id, std::vector<Task>* tasks)
            -> StorageErr
            = 0;

    virtual auto update_heartbeat(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto
    heartbeat_timeout(StorageConnection& conn, double timeout, std::vector<boost::uuids::uuid>* ids)
            -> StorageErr
            = 0;

    virtual auto
    get_scheduler_addr(StorageConnection& conn, boost::uuids::uuid id, std::string* addr, int* port)
            -> StorageErr
            = 0;
};
}  // namespace spider::core
#endif  // SPIDER_STORAGE_METADATASTORAGE_HPP
