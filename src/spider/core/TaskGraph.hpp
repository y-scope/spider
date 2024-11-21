#ifndef SPIDER_CORE_TASKGRAPH_HPP
#define SPIDER_CORE_TASKGRAPH_HPP

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <optional>
#include <utility>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "Task.hpp"

namespace spider::core {
class TaskGraph {
public:
    TaskGraph() = default;

    auto add_child_task(Task const& task, std::vector<boost::uuids::uuid> const& parents) -> bool {
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
    auto add_task(Task const& task) -> bool {
        boost::uuids::uuid const task_id = task.get_id();
        if (m_tasks.contains(task_id)) {
            return false;
        }
        m_tasks.emplace(task_id, task);
        return true;
    }

    void add_dependency(boost::uuids::uuid parent, boost::uuids::uuid child) {
        m_dependencies.emplace_back(parent, child);
    }

    [[nodiscard]] auto get_task(boost::uuids::uuid id) const -> std::optional<Task const*> {
        if (m_tasks.contains(id)) {
            return &m_tasks.at(id);
        }
        return std::nullopt;
    }

    [[nodiscard]] auto get_task(boost::uuids::uuid id) -> std::optional<Task*> {
        if (m_tasks.contains(id)) {
            return &m_tasks.at(id);
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

    [[nodiscard]] auto get_head_tasks() const -> absl::flat_hash_set<boost::uuids::uuid> {
        absl::flat_hash_set<boost::uuids::uuid> heads;
        for (auto const& pair : m_tasks) {
            heads.emplace(pair.first);
        }
        for (auto const& pair : m_dependencies) {
            heads.erase(pair.second);
        }
        return heads;
    }

    [[nodiscard]] auto get_dependencies(
    ) const -> std::vector<std::pair<boost::uuids::uuid, boost::uuids::uuid>> const& {
        return m_dependencies;
    }

private:
    absl::flat_hash_map<boost::uuids::uuid, Task> m_tasks;
    std::vector<std::pair<boost::uuids::uuid, boost::uuids::uuid>> m_dependencies;
};
}  // namespace spider::core

#endif  // SPIDER_CORE_TASKGRAPH_HPP
