#ifndef SPIDER_CORE_JOBRECOVERY_HPP
#define SPIDER_CORE_JOBRECOVERY_HPP

#include <memory>

#include <boost/uuid/uuid.hpp>

#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"

namespace spider::core {
class JobRecovery {
public:
    JobRecovery(
            boost::uuids::uuid job_id,
            std::shared_ptr<StorageConnection> storage_connection,
            std::shared_ptr<DataStorage> data_store,
            std::shared_ptr<MetadataStorage> metadata_store
    );

    auto compute_graph() -> StorageErr;

    auto get_ready_tasks() -> std::vector<boost::uuids::uuid>;

    auto get_pending_tasks() -> std::vector<boost::uuids::uuid>;

private:

    /**
     * Check if all the task input data are persisted.
     * @param persisted True if all the task input data are persisted, false otherwise.
     * @return The storage error code from accessing the storage.
     */
    auto check_task_input(bool& persisted) -> StorageErr;

    boost::uuids::uuid m_job_id;

    std::shared_ptr<StorageConnection> m_conn;
    std::shared_ptr<DataStorage> m_data_store;
    std::shared_ptr<MetadataStorage> m_metadata_store;

    TaskGraph m_task_graph;
};
}  // namespace spider::core

#endif
