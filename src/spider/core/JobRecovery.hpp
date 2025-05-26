#ifndef SPIDER_CORE_JOBRECOVERY_HPP
#define SPIDER_CORE_JOBRECOVERY_HPP

#include <memory>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/uuid/uuid.hpp>

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

    auto get_ready_tasks() -> std::vector<boost::uuids::uuid> const&;

    auto get_pending_tasks() -> std::vector<boost::uuids::uuid> const&;

private:
    /**
     * Check if any of the task input is not persisted.
     * @param task
     * @param not_persisted Returns true if any of the task input is not
     * persisted, false otherwise.
     * @return
     */
    auto check_task_input(Task const& task, bool& not_persisted) -> StorageErr;

    /**
     * Get the data associated with the given data_id. If the data is cached in
     * m_data_map, return it. Otherwise, fetch it from the data store and cache
     * it.
     * @param data_id
     * @param data
     * @return
     */
    auto get_data(boost::uuids::uuid data_id, Data& data) -> StorageErr;

    boost::uuids::uuid m_job_id;

    std::shared_ptr<StorageConnection> m_conn;
    std::shared_ptr<DataStorage> m_data_store;
    std::shared_ptr<MetadataStorage> m_metadata_store;

    absl::flat_hash_map<boost::uuids::uuid, Data> m_data_map;

    TaskGraph m_task_graph;

    std::vector<boost::uuids::uuid> m_ready_tasks;
    std::vector<boost::uuids::uuid> m_pending_tasks;
};
}  // namespace spider::core

#endif
