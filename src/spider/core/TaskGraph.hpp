#ifndef SPIDER_CORE_TASKGRAPH_HPP
#define SPIDER_CORE_TASKGRAPH_HPP

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <optional>
#include <utility>
#include <vector>

#include "Task.hpp"

namespace spider::core {
class TaskGraph {
public:
    TaskGraph() {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

    explicit TaskGraph(boost::uuids::uuid id) : m_id(id) {}

    bool add_child_task(Task const& task, std::vector<boost::uuids::uuid> const& parents) {
        boost::uuids::uuid task_id = task.get_id();
        for (boost::uuids::uuid const parent_id : parents) {
            if (!m_tasks.contains(parent_id)) {
                return false;
            }
        }
        if (m_tasks.contains(task_id)) {
            return false;
        }

        m_tasks.emplace(task_id, task);
        for (boost::uuids::uuid const parent_id : parents) {
            m_dependencies.emplace_back(parent_id, task_id);
        }
        return true;
    }

    // User is responsible to add the dependencies
    bool add_task(Task const& task) {
        boost::uuids::uuid task_id = task.get_id();
        if (m_tasks.contains(task.get_id())) {
            return false;
        }
        m_tasks.emplace(task.get_id(), task);
        return true;
    }

    void add_dependencies(boost::uuids::uuid parent, boost::uuids::uuid child) {
        m_dependencies.emplace_back(parent, child);
    }

    boost::uuids::uuid get_id() const { return m_id; }

    std::optional<Task> get_task(boost::uuids::uuid id) const {
        if (m_tasks.contains(id)) {
            return m_tasks.at(id);
        }
        return std::nullopt;
    }

    [[nodiscard]] auto get_child_tasks(boost::uuids::uuid id
    ) const -> std::vector<boost::uuids::uuid> {
        std::vector<boost::uuids::uuid> children;
        for (std::pair<boost::uuids::uuid, boost::uuids::uuid> const dep : m_dependencies) {
            if (dep.first == id) {
                children.emplace_back(dep.second);
            }
        }
        return children;
    }

    [[nodiscard]] auto get_parent_tasks(boost::uuids::uuid id
    ) const -> std::vector<boost::uuids::uuid> {
        std::vector<boost::uuids::uuid> parents;
        for (std::pair<boost::uuids::uuid, boost::uuids::uuid> const dep : m_dependencies) {
            if (dep.second == id) {
                parents.emplace_back(dep.first);
            }
        }
        return parents;
    }

    [[nodiscard]] auto get_tasks() const -> absl::flat_hash_map<boost::uuids::uuid, Task> const& {
        return m_tasks;
    }

    absl::flat_hash_set<boost::uuids::uuid> get_head_tasks() const {
        absl::flat_hash_set<boost::uuids::uuid> heads;
        for (auto const& pair : m_tasks) {
            heads.emplace(pair.first);
        }
        for (auto const& pair : m_dependencies) {
            heads.erase(pair.second);
        }
        return heads;
    }

    std::vector<std::pair<boost::uuids::uuid, boost::uuids::uuid>> const& get_dependencies() const {
        return m_dependencies;
    }

private:
    boost::uuids::uuid m_id;
    absl::flat_hash_map<boost::uuids::uuid, Task> m_tasks;
    std::vector<std::pair<boost::uuids::uuid, boost::uuids::uuid>> m_dependencies;
};
}  // namespace spider::core

#endif  // SPIDER_CORE_TASKGRAPH_HPP
