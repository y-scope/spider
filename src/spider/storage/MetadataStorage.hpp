#ifndef SPIDER_STORAGE_METADATASTORAGE_HPP
#define SPIDER_STORAGE_METADATASTORAGE_HPP

#include <boost/uuid/uuid.hpp>
#include <string>
#include <vector>

#include "../core/Error.hpp"
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

    virtual auto connect(std::string url, boost::uuids::uuid id) -> StorageErr = 0;
    virtual void close() = 0;
    virtual auto initialize() -> StorageErr = 0;

    virtual auto add_task_graph(TaskGraph const& task_graph) -> StorageErr = 0;
    virtual auto get_task_graph(boost::uuids::uuid id, TaskGraph& task_graph) -> StorageErr = 0;
    virtual auto get_task_graphs(std::vector<TaskGraph>& task_graphs) -> StorageErr = 0;
    virtual auto remove_task_graph(boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto add_child(boost::uuids::uuid parent_id, Task const& child) -> StorageErr = 0;
    virtual auto get_task(boost::uuids::uuid id, Task& task) -> StorageErr = 0;
    virtual auto get_ready_tasks(std::vector<Task>& tasks) -> StorageErr = 0;
    virtual auto set_task_state(boost::uuids::uuid id, TaskState state) -> StorageErr = 0;
    virtual auto add_task_instance(TaskInstance const& instance) -> StorageErr = 0;
    virtual auto task_finish(TaskInstance const& instance) -> StorageErr = 0;
    virtual auto get_task_timeout(std::vector<Task>& tasks) -> StorageErr = 0;
    virtual auto get_child_task(boost::uuids::uuid id, Task& child) -> StorageErr = 0;
    virtual auto get_parent_tasks(boost::uuids::uuid id, std::vector<Task>& tasks) -> StorageErr
                                                                                      = 0;

    virtual auto update_heartbeat(boost::uuids::uuid id) -> StorageErr = 0;
    virtual auto heartbeat_timeout(std::vector<boost::uuids::uuid>& ids) -> StorageErr = 0;
};

}  // namespace spider::core
#endif  // SPIDER_STORAGE_METADATASTORAGE_HPP
