#ifndef SPIDER_CORE_JOBRECOVERY_HPP
#define SPIDER_CORE_JOBRECOVERY_HPP

#include <deque>
#include <memory>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/uuid/uuid.hpp>

#include <spider/core/Data.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/Task.hpp>
#include <spider/core/TaskGraph.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>

namespace spider::core {
class JobRecovery {
public:
    JobRecovery(
            boost::uuids::uuid job_id,
            std::shared_ptr<StorageConnection> storage_connection,
            std::shared_ptr<DataStorage> data_store,
            std::shared_ptr<MetadataStorage> metadata_store
    );

    /**
     * Recover the job by loading the task graph and data from the storage,
     * compute the minimal subgraph that contains all the failed tasks and the
     * data across edge are all persisted.
     * The result is stored in m_ready_tasks and m_pending_tasks, where
     * m_ready_tasks contains the tasks on the boundary of the subgraph, and
     * m_pending_tasks contains the tasks that are not ready to run yet.
     * @return StorageErr
     */
    auto compute_graph() -> StorageErr;

    auto get_ready_tasks() -> std::vector<boost::uuids::uuid>;

    auto get_pending_tasks() -> std::vector<boost::uuids::uuid>;

private:
    /**
     * Check if task has any parents with non-persisted Data that feed into the task.
     * @param task
     * @param not_persisted Returns parents with non-persisted Data that feed into the task.
     * @return
     */
    auto check_task_input(Task const& task, absl::flat_hash_set<boost::uuids::uuid>& not_persisted)
            -> StorageErr;

    /**
     * Get the data associated with the given data_id. If the data is cached in
     * m_data_map, return it. Otherwise, fetch it from the data store and cache
     * it.
     * @param data_id
     * @param data
     * @return
     */
    auto get_data(boost::uuids::uuid data_id, Data& data) -> StorageErr;

    /*
     * Process the task from the task queue with the given task_id.
     * 1. Add the non-pending children of the task to the working queue.
     * 2. Check if its inputs contains non-persisted Data.
     * 3. If the task has non-persisted Data input and has parents, add it to pending tasks and add
     * its parents with non-persistent Data to the working queue.
     * 4. Otherwise, add it to ready tasks.
     *
     * @param task_id
     * @return StorageErr
     */
    auto process_task(boost::uuids::uuid task_id) -> StorageErr;

    boost::uuids::uuid m_job_id;

    std::shared_ptr<StorageConnection> m_conn;
    std::shared_ptr<DataStorage> m_data_store;
    std::shared_ptr<MetadataStorage> m_metadata_store;

    absl::flat_hash_map<boost::uuids::uuid, Data> m_data_map;

    TaskGraph m_task_graph;

    absl::flat_hash_set<boost::uuids::uuid> m_task_set;
    std::deque<boost::uuids::uuid> m_task_queue;
    absl::flat_hash_set<boost::uuids::uuid> m_ready_tasks;
    absl::flat_hash_set<boost::uuids::uuid> m_pending_tasks;
};
}  // namespace spider::core

#endif
