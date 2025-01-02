#ifndef SPIDER_CORE_TASKGRAPHIMPL_HPP
#define SPIDER_CORE_TASKGRAPHIMPL_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>

#include <boost/uuid/uuid.hpp>

#include "../client/Data.hpp"
#include "../client/task.hpp"
#include "../client/TaskContext.hpp"
#include "../client/type_utils.hpp"
#include "../core/Task.hpp"
#include "../core/TaskGraph.hpp"
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp"  // IWYU pragma: keep
#include "../worker/FunctionManager.hpp"

namespace spider::core {

class TaskGraphImpl {
public:
    // NOLINTBEGIN(readability-function-cognitive-complexity, cppcoreguidelines-missing-std-forward)
    template <TaskIo ReturnType, TaskIo... TaskParams, RunnableOrTaskIo... Inputs>
    static auto
    bind(TaskFunction<ReturnType, TaskParams...> const& task_function,
         Inputs&&... inputs) -> std::optional<TaskGraphImpl> {
        std::optional<Task> optional_task = create_task(task_function);
        if (!optional_task.has_value()) {
            return std::nullopt;
        }
        Task& task = optional_task.value();
        TaskGraphImpl graph;

        uint64_t position = 0;
        bool fail = false;
        for_n<sizeof...(Inputs)>([&](auto i) {
            if (fail) {
                return;
            }
            using InputType
                    = std::remove_cvref_t<std::tuple_element_t<i.cValue, std::tuple<Inputs...>>>;
            if constexpr (std::is_pointer_v<InputType>
                          && std::is_function_v<std::remove_pointer_t<InputType>>)
            {
                std::optional<Task> optional_parent = add_task_input(
                        task,
                        std::get<i.cValue>(std::forward_as_tuple(inputs...)),
                        position
                );
                if (!optional_parent.has_value()) {
                    fail = true;
                }
                graph.m_graph.add_task(optional_parent.value());
                graph.m_graph.add_input_task(optional_parent.value().get_id());
            } else if constexpr (cIsSpecializationV<InputType, spider::TaskGraph>) {
                TaskGraph parent_graph
                        = std::get<i.cValue>(std::forward_as_tuple(inputs...)).get_impl().m_graph;
                parent_graph.reset_ids();
                if (!add_graph_input(task, parent_graph, position)) {
                    fail = true;
                }
                for (boost::uuids::uuid const& intput_task_id : parent_graph.get_input_tasks()) {
                    graph.m_graph.add_input_task(intput_task_id);
                }
                for (auto const& [task_id, task] : parent_graph.get_tasks()) {
                    graph.m_graph.add_task(task);
                }
            } else if constexpr (TaskIo<InputType>) {
                if (position >= task.get_num_inputs()) {
                    fail = true;
                }
                TaskInput& input = task.get_input_ref(position);
                position++;
                // Check type match
                if (input.get_type() != typeid(InputType).name()) {
                    fail = true;
                }
                if constexpr (cIsSpecializationV<InputType, spider::Data>) {
                    input.set_data_id(std::get<i.cValue>(std::forward_as_tuple(inputs...))
                                              .get_impl()
                                              ->get_id());
                } else if constexpr (Serializable<InputType>) {
                    msgpack::sbuffer buffer;
                    msgpack::pack(buffer, std::get<i.cValue>(std::forward_as_tuple(inputs...)));
                    std::string const value(buffer.data(), buffer.size());
                    input.set_value(value);
                }
            }
        });
        if (fail) {
            return std::nullopt;
        }

        // Check all inputs are consumed
        if (position != task.get_num_inputs()) {
            return std::nullopt;
        }

        graph.m_graph.add_task(task);
        graph.m_graph.add_output_task(task.get_id());
        return graph;
    }

    // NOLINTEND(readability-function-cognitive-complexity, cppcoreguidelines-missing-std-forward)

private:
    template <TaskIo ReturnType, TaskIo... TaskParams>
    static auto create_task(TaskFunction<ReturnType, TaskParams...> const& task_function
    ) -> std::optional<Task> {
        // NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
        std::optional<std::string> const function_name
                = FunctionManager::get_instance().get_function_name(
                        reinterpret_cast<void const*>(task_function)
                );
        // NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
        if (!function_name.has_value()) {
            return std::nullopt;
        }
        Task task{function_name.value()};
        // Add task inputs
        ((task.add_input(TaskInput{typeid(TaskParams).name()})), ...);
        // Add task outputs
        if constexpr (cIsSpecializationV<ReturnType, std::tuple>) {
            for_n<std::tuple_size_v<ReturnType>>([&](auto i) {
                task.add_output(TaskOutput{typeid(std::tuple_element_t<i.cValue, ReturnType>).name()
                });
            });
        } else {
            task.add_output(TaskOutput{typeid(ReturnType).name()});
        }
        return task;
    }

    template <class ReturnType, class... TaskParams>
    static auto add_task_input(
            Task& task,
            TaskFunction<ReturnType, TaskParams...> const& task_function,
            uint64_t& position
    ) -> std::optional<Task> {
        std::optional<Task> const optional_parent = create_task(task_function);
        if (!optional_parent.has_value()) {
            return std::nullopt;
        }
        Task const& parent = optional_parent.value();
        if constexpr (cIsSpecializationV<ReturnType, std::tuple>) {
            for_n<std::tuple_size_v<ReturnType>>([&](auto i) {
                if (position >= sizeof...(TaskParams)) {
                    return std::nullopt;
                }
                TaskInput& input = task.get_input_ref(position);
                position++;
                input.set_output(parent.get_id(), i.cValue);
            });
        } else {
            if (position >= sizeof...(TaskParams)) {
                return std::nullopt;
            }
            TaskInput& input = task.get_input_ref(position);
            position++;
            input.set_output(parent.get_id(), 0);
        }
        return parent;
    }

    static auto add_graph_input(Task& task, TaskGraph const& graph, uint64_t& position) -> bool {
        for (boost::uuids::uuid const& output_task_id : graph.get_output_tasks()) {
            std::optional<Task const*> optional_output_task = graph.get_task(output_task_id);
            if (!optional_output_task.has_value()) {
                return false;
            }
            Task const& output_task = *optional_output_task.value();
            for (int64_t i = 0; i < output_task.get_num_outputs(); i++) {
                if (position >= task.get_num_inputs()) {
                    return false;
                }
                TaskInput& input = task.get_input_ref(position);
                position++;
                // Check type match
                TaskOutput const& output = output_task.get_output(i);
                if (input.get_type() != output.get_type()) {
                    return false;
                }
                input.set_output(output_task_id, i);
            }
        }
        return true;
    }

    TaskGraph m_graph;
};

}  // namespace spider::core

#endif  // SPIDER_CORE_TASKGRAPHIMPL_HPP
