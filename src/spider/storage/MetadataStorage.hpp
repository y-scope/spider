#ifndef SPIDER_STORAGE_METADATASTORAGE_HPP
#define SPIDER_STORAGE_METADATASTORAGE_HPP

#include <boost/uuid/uuid.hpp>
#include <string>

#include "../core/Error.hpp"
#include "../core/Task.hpp"
#include "../core/TaskGraph.hpp"

namespace spider::core {
class MetadataStorage {
public:
    virtual StorageErr connect(std::string url, boost::uuids::uuid id) = 0;
    virtual void close() = 0;
    virtual StorageErr initialize() = 0;

    virtual StorageErr add_task_graph(TaskGraph const& task_graph) = 0;
    virtual StorageErr get_task_graph(boost::uuids::uuid id, TaskGraph& task_graph) = 0;
    virtual StorageErr get_task_graphs(std::vector<TaskGraph>& task_graphs) = 0;
    virtual StorageErr remove_task_graph(boost::uuids::uuid id) = 0;
    virtual StorageErr add_child(boost::uuids::uuid parent_id, Task const& child) = 0;
    virtual StorageErr get_task(boost::uuids::uuid id, Task& task) = 0;
    virtual StorageErr get_ready_tasks(std::vector<Task>& tasks) = 0;
    virtual StorageErr set_task_state(boost::uuids::uuid id, TaskState state) = 0;
    virtual StorageErr add_task_instance(TaskInstance const& instance) = 0;
    virtual StorageErr task_finish(TaskInstance const& instance) = 0;
    virtual StorageErr get_task_timeout(std::vector<Task>& tasks) = 0;
    virtual StorageErr get_child_task(boost::uuids::uuid id, Task& child) = 0;
    virtual StorageErr get_parent_tasks(boost::uuids::uuid id, std::vector<Task>& tasks) = 0;

    virtual StorageErr update_heartbeat(boost::uuids::uuid id) = 0;
    virtual StorageErr hearbeat_timeout(std::vector<boost::uuids::uuid>& ids) = 0;
};

}  // namespace spider::core
#endif  // SPIDER_STORAGE_METADATASTORAGE_HPP
