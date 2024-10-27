#ifndef SPIDER_CORE_TASK_HPP
#define SPIDER_CORE_TASK_HPP

#include <boost/uuid/random_generator.hpp>
#include <string>

namespace spider::core {

class TaskInput {
private:
    std::optional<std::tuple<boost::uuids::uuid, uint8_t>> m_task_output;
    std::optional<std::string> m_value;
    std::optional<boost::uuids::uuid> m_data_id;
    std::string m_type;

public:
    TaskInput(boost::uuids::uuid output_task_id, uint8_t position, std::string type)
            : m_task_output({output_task_id, position}),
              m_type(type) {};
    TaskInput(std::string value, std::string type) : m_value(value), m_type(type) {};
    TaskInput(boost::uuids::uuid data_id, std::string type) : m_data_id(data_id), m_type(type) {};

    std::optional<std::tuple<boost::uuids::uuid, uint8_t>> get_task_output() const {
        return m_task_output;
    }
    std::optional<std::string> get_value() const { return m_value; }
    std::optional<boost::uuids::uuid> get_data_id() const { return m_data_id; }
    std::string get_type() const { return m_type; }
};

class TaskOutput {
private:
    std::optional<std::string> m_value;
    std::optional<boost::uuids::uuid> m_data_id;
    std::string m_type;

public:
    TaskOutput(std::string value, std::string type) : m_value(value), m_type(type) {}

    TaskOutput(boost::uuids::uuid data_id, std::string type) : m_data_id(data_id), m_type(type) {}

    std::optional<std::string> get_value() const { return m_value; }
    std::optional<boost::uuids::uuid> get_data_id() const { return m_data_id; }
    std::string get_type() const { return m_type; }
};

class TaskInstance {};

enum TaskState {
    kPending = 0,
    kReady,
    kRunning,
    kSucceed,
    kFailed,
    kCanceled,
};

enum TaskCreatorType {
    kClient = 0,
    kTask,
};

class Task {
private:
    boost::uuids::uuid m_id;
    std::string m_function_name;
    TaskState m_state = kPending;
    TaskCreatorType m_creator_type;
    boost::uuids::uuid m_creator_id;
    float m_timeout = 0;
    std::vector<TaskInput> m_inputs;
    std::vector<TaskOutput> m_outputs;

public:
    Task(std::string function_name, TaskCreatorType creator_type, boost::uuids::uuid creator_id)
            : m_function_name(std::move(function_name)),
              m_creator_type(creator_type),
              m_creator_id(creator_id) {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

    void add_input(TaskInput const& input) { m_inputs.emplace_back(input); }

    void add_output(TaskOutput const& output) { m_outputs.emplace_back(output); }

    boost::uuids::uuid get_id() const { return m_id; }
    std::string get_function_name() const { return m_function_name; }
    TaskState get_state() const { return m_state; }
    TaskCreatorType get_creator_type() const { return m_creator_type; }
    boost::uuids::uuid get_creator_id() const { return m_creator_id; }
    float get_timeout() const { return m_timeout; }

    uint64_t get_num_inputs() const { return m_inputs.size(); }
    uint64_t get_num_outputs() const { return m_outputs.size(); }
    TaskInput get_input(uint64_t index) const { return m_inputs[index]; }
    TaskInput get_output(uint64_t index) const { return m_outputs[index]; }
};

}  // namespace spider::core

#endif  // SPIDER_CORE_TASK_HPP
