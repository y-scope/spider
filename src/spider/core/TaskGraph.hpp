#ifndef SPIDER_CORE_TASKGRAPH_HPP
#define SPIDER_CORE_TASKGRAPH_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/uuid/random_generator.hpp>
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

    [[nodiscard]] auto get_input_tasks() const -> std::vector<boost::uuids::uuid> const& {
        return m_input_tasks;
    }

    [[nodiscard]] auto get_output_tasks() const -> std::vector<boost::uuids::uuid> const& {
        return m_output_tasks;
    }

    auto add_input_task(boost::uuids::uuid id) -> void { m_input_tasks.emplace_back(id); }

    auto add_output_task(boost::uuids::uuid id) -> void { m_output_tasks.emplace_back(id); }

    [[nodiscard]] auto get_dependencies(
    ) const -> std::vector<std::pair<boost::uuids::uuid, boost::uuids::uuid>> const& {
        return m_dependencies;
    }

    auto reset_ids() -> void {
        absl::flat_hash_map<boost::uuids::uuid, boost::uuids::uuid> new_id_map;
        boost::uuids::random_generator gen;
        for (auto const& [old_id, task] : m_tasks) {
            boost::uuids::uuid new_id = gen();
            new_id_map.emplace(old_id, new_id);
        }
        // Replace all id in task map and task
        absl::flat_hash_map<boost::uuids::uuid, Task> new_tasks;
        for (auto& [old_id, task] : m_tasks) {
            boost::uuids::uuid new_id = new_id_map.at(old_id);
            task.set_id(new_id);
            for (size_t i = 0; i < task.get_num_inputs(); i++) {
                TaskInput& input = task.get_input_ref(i);
                std::optional<std::tuple<boost::uuids::uuid, uint8_t>> const& optional_task_output
                        = input.get_task_output();
                if (optional_task_output.has_value()) {
                    boost::uuids::uuid const task_id = std::get<0>(optional_task_output.value());
                    input.set_output(
                            new_id_map.at(task_id),
                            std::get<1>(optional_task_output.value())
                    );
                }
            }
            new_tasks.emplace(new_id, std::move(task));
        }
        m_tasks = std::move(new_tasks);

        // Replace all id in dependencies
        for (auto& dep : m_dependencies) {
            dep.first = new_id_map.at(dep.first);
            dep.second = new_id_map.at(dep.second);
        }

        // Replace all id in input and output tasks
        for (auto& task : m_input_tasks) {
            task = new_id_map.at(task);
        }
        for (auto& task : m_output_tasks) {
            task = new_id_map.at(task);
        }
    }

private:
    absl::flat_hash_map<boost::uuids::uuid, Task> m_tasks;
    std::vector<std::pair<boost::uuids::uuid, boost::uuids::uuid>> m_dependencies;

    std::vector<boost::uuids::uuid> m_input_tasks;
    std::vector<boost::uuids::uuid> m_output_tasks;
};
}  // namespace spider::core

#endif  // SPIDER_CORE_TASKGRAPH_HPP
