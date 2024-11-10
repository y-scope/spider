#ifndef SPIDER_CORE_TASK_HPP
#define SPIDER_CORE_TASK_HPP

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace spider::core {
class TaskInput {
public:
    TaskInput(boost::uuids::uuid output_task_id, std::uint8_t position, std::string type)
            : m_task_output({output_task_id, position}),
              m_type(std::move(type)) {};
    TaskInput(std::string value, std::string type)
            : m_value(std::move(value)),
              m_type(std::move(type)) {};
    TaskInput(boost::uuids::uuid data_id, std::string type)
            : m_data_id(data_id),
              m_type(std::move(type)) {};

    [[nodiscard]] auto get_task_output(
    ) const -> std::optional<std::tuple<boost::uuids::uuid, std::uint8_t>> {
        return m_task_output;
    }

    [[nodiscard]] auto get_value() const -> std::optional<std::string> { return m_value; }

    [[nodiscard]] auto get_data_id() const -> std::optional<boost::uuids::uuid> {
        return m_data_id;
    }

    [[nodiscard]] auto get_type() const -> std::string { return m_type; }

    void set_value(std::string const& value) { m_value = value; }

    void set_data_id(boost::uuids::uuid data_id) { m_data_id = data_id; }

private:
    std::optional<std::tuple<boost::uuids::uuid, std::uint8_t>> m_task_output;
    std::optional<std::string> m_value;
    std::optional<boost::uuids::uuid> m_data_id;
    std::string m_type;
};

class TaskOutput {
public:
    explicit TaskOutput(std::string type) : m_type(std::move(type)) {}

    TaskOutput(std::string value, std::string type)
            : m_value(std::move(value)),
              m_type(std::move(type)) {}

    TaskOutput(boost::uuids::uuid data_id, std::string type)
            : m_data_id(data_id),
              m_type(std::move(type)) {}

    [[nodiscard]] auto get_value() const -> std::optional<std::string> { return m_value; }

    [[nodiscard]] auto get_data_id() const -> std::optional<boost::uuids::uuid> {
        return m_data_id;
    }

    [[nodiscard]] auto get_type() const -> std::string { return m_type; }

    void set_value(std::string const& value) { m_value = value; }

    void set_data_id(boost::uuids::uuid data_id) { m_data_id = data_id; }

private:
    std::optional<std::string> m_value;
    std::optional<boost::uuids::uuid> m_data_id;
    std::string m_type;
};

struct TaskInstance {
    boost::uuids::uuid id;
    boost::uuids::uuid task_id;

    explicit TaskInstance(boost::uuids::uuid task_id) : task_id(task_id) {
        boost::uuids::random_generator gen;
        id = gen();
    }

    TaskInstance(boost::uuids::uuid id, boost::uuids::uuid task_id) : id(id), task_id(task_id) {}
};

enum class TaskState : std::uint8_t {
    Pending,
    Ready,
    Running,
    Succeed,
    Failed,
    Canceled,
};

class Task {
public:
    explicit Task(std::string function_name) : m_function_name(std::move(function_name)) {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

    Task(boost::uuids::uuid id, std::string function_name, TaskState state, float timeout)
            : m_id(id),
              m_function_name(std::move(function_name)),
              m_state(state),
              m_timeout(timeout) {}

    void add_input(TaskInput const& input) { m_inputs.emplace_back(input); }

    void add_output(TaskOutput const& output) { m_outputs.emplace_back(output); }

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid { return m_id; }

    [[nodiscard]] auto get_function_name() const -> std::string { return m_function_name; }

    [[nodiscard]] auto get_state() const -> TaskState { return m_state; }

    [[nodiscard]] auto get_timeout() const -> float { return m_timeout; }

    [[nodiscard]] auto get_num_inputs() const -> size_t { return m_inputs.size(); }

    [[nodiscard]] auto get_num_outputs() const -> size_t { return m_outputs.size(); }

    [[nodiscard]] auto get_input(uint64_t index) const -> TaskInput { return m_inputs[index]; }

    [[nodiscard]] auto get_output(uint64_t index) const -> TaskOutput { return m_outputs[index]; }

    [[nodiscard]] auto get_inputs() const -> std::vector<TaskInput> const& { return m_inputs; }

    [[nodiscard]] auto get_outputs() const -> std::vector<TaskOutput> const& { return m_outputs; }

private:
    boost::uuids::uuid m_id;
    std::string m_function_name;
    TaskState m_state = TaskState::Pending;
    float m_timeout = 0;
    std::vector<TaskInput> m_inputs;
    std::vector<TaskOutput> m_outputs;
};

}  // namespace spider::core

#endif  // SPIDER_CORE_TASK_HPP
