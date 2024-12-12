#include "MysqlStorage.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <deque>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_set.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>
#include <mariadb/conncpp/CArray.hpp>
#include <mariadb/conncpp/Driver.hpp>
#include <mariadb/conncpp/Exception.hpp>
#include <mariadb/conncpp/jdbccompat.hpp>
#include <mariadb/conncpp/PreparedStatement.hpp>
#include <mariadb/conncpp/Properties.hpp>
#include <mariadb/conncpp/ResultSet.hpp>
#include <mariadb/conncpp/Statement.hpp>

#include "../core/Data.hpp"
#include "../core/Driver.hpp"
#include "../core/Error.hpp"
#include "../core/JobMetadata.hpp"
#include "../core/KeyValueData.hpp"
#include "../core/Task.hpp"
#include "../core/TaskGraph.hpp"

// mariadb-connector-cpp does not define SQL errcode. Just include some useful ones.
enum MariadbErr : uint16_t {
    ErCantCreateTable = 1005,
    ErCantCreateDb = 1006,
    ErDupKey = 1022,
    ErKeyNotFound = 1032,
    ErDupEntry = 1062,
    ErWrongDbName = 1102,
    ErWrongTableName = 1103,
    ErUnknownTable = 1109,
};

namespace spider::core {
namespace {
char const* const cCreateDriverTable = R"(CREATE TABLE IF NOT EXISTS `drivers` (
    `id` BINARY(16) NOT NULL,
    `address` VARCHAR(40) NOT NULL,
    `heartbeat` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
))";

char const* const cCreateSchedulerTable = R"(CREATE TABLE IF NOT EXISTS `schedulers` (
    `id` BINARY(16) NOT NULL,
    `port` INT UNSIGNED NOT NULL,
    `state` ENUM('normal', 'recovery', 'gc') NOT NULL,
    CONSTRAINT `scheduler_driver_id` FOREIGN KEY (`id`) REFERENCES `drivers` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
))";

char const* const cCreateJobTable = R"(CREATE TABLE IF NOT EXISTS jobs (
    `id` BINARY(16) NOT NULL,
    `client_id` BINARY(16) NOT NULL,
    `creation_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY (`client_id`) USING BTREE,
    PRIMARY KEY (`id`)
))";

char const* const cCreateTaskTable = R"(CREATE TABLE IF NOT EXISTS tasks (
    `id` BINARY(16) NOT NULL,
    `job_id` BINARY(16) NOT NULL,
    `func_name` VARCHAR(64) NOT NULL,
    `state` ENUM('pending', 'ready', 'running', 'success', 'cancel', 'fail') NOT NULL,
    `timeout` FLOAT,
    `max_retry` INT UNSIGNED DEFAULT 0,
    `instance_id` BINARY(16),
    CONSTRAINT `task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
))";

char const* const cCreateTaskInputTable = R"(CREATE TABLE IF NOT EXISTS `task_inputs` (
    `task_id` BINARY(16) NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    `type` VARCHAR(64) NOT NULL,
    `output_task_id` BINARY(16),
    `output_task_position` INT UNSIGNED,
    `value` VARCHAR(64), -- Use VARCHAR for all types of values
    `data_id` BINARY(16),
    CONSTRAINT `input_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `input_task_output_match` FOREIGN KEY (`output_task_id`, `output_task_position`) REFERENCES task_outputs (`task_id`, `position`) ON UPDATE NO ACTION ON DELETE SET NULL,
    CONSTRAINT `input_data_id` FOREIGN KEY (`data_id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION,
    PRIMARY KEY (`task_id`, `position`)
))";

char const* const cCreateTaskOutputTable = R"(CREATE TABLE IF NOT EXISTS `task_outputs` (
    `task_id` BINARY(16) NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    `type` VARCHAR(64) NOT NULL,
    `value` VARCHAR(64),
    `data_id` BINARY(16),
    CONSTRAINT `output_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `output_data_id` FOREIGN KEY (`data_id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION,
    PRIMARY KEY (`task_id`, `position`)
))";

char const* const cCreateTaskDependencyTable = R"(CREATE TABLE IF NOT EXISTS `task_dependencies` (
    `parent` BINARY(16) NOT NULL,
    `child` BINARY(16) NOT NULL,
    KEY (`parent`) USING BTREE,
    KEY (`child`) USING BTREE,
    CONSTRAINT `task_dep_parent` FOREIGN KEY (`parent`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `task_dep_child` FOREIGN KEY (`child`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

char const* const cCreateTaskInstanceTable = R"(CREATE TABLE IF NOT EXISTS `task_instances` (
    `id` BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    `start_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT `instance_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
))";

char const* const cCreateDataTable = R"(CREATE TABLE IF NOT EXISTS `data` (
    `id` BINARY(16) NOT NULL,
    `value` VARCHAR(256) NOT NULL,
    `hard_locality` BOOL DEFAULT FALSE,
    `persisted` BOOL DEFAULT FALSE,
    PRIMARY KEY (`id`)
))";

char const* const cCreateDataLocalityTable = R"(CREATE TABLE IF NOT EXISTS `data_locality` (
    `id` BINARY(16) NOT NULL,
    `address` VARCHAR(40) NOT NULL,
    KEY (`id`) USING BTREE,
    CONSTRAINT `locality_data_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

char const* const cCreateDataRefDriverTable = R"(CREATE TABLE IF NOT EXISTS `data_ref_driver` (
    `id` BINARY(16) NOT NULL,
    `driver_id` BINARY(16) NOT NULL,
    KEY (`id`) USING BTREE,
    KEY (`driver_id`) USING BTREE,
    CONSTRAINT `data_driver_ref_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `data_ref_driver_id` FOREIGN KEY (`driver_id`) REFERENCES `drivers` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

char const* const cCreateDataRefTaskTable = R"(CREATE TABLE IF NOT EXISTS `data_ref_task` (
    `id` BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    KEY (`id`) USING BTREE,
    KEY (`task_id`) USING BTREE,
    CONSTRAINT `data_task_ref_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `data_ref_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

char const* const cCreateClientKVDataTable = R"(CREATE TABLE IF NOT EXISTS `client_kv_data` (
    `kv_key` VARCHAR(64) NOT NULL,
    `value` VARCHAR(128) NOT NULL,
    `client_id` BINARY(16) NOT NULL,
    PRIMARY KEY (`client_id`, `kv_key`)
))";

char const* const cCreateTaskKVDataTable = R"(CREATE TABLE IF NOT EXISTS `task_kv_data` (
    `kv_key` VARCHAR(64) NOT NULL,
    `value` VARCHAR(128) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    PRIMARY KEY (`task_id`, `kv_key`),
    CONSTRAINT `kv_data_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

std::array<char const* const, 14> const cCreateStorage = {
        cCreateDriverTable,  // drivers table must be created before data_ref_driver
        cCreateSchedulerTable,
        cCreateJobTable,  // jobs table must be created before task
        cCreateTaskTable,  // tasks table must be created before data_ref_task
        cCreateDataTable,  // data table must be created before task_outputs
        cCreateDataLocalityTable,
        cCreateDataRefDriverTable,
        cCreateDataRefTaskTable,
        cCreateClientKVDataTable,
        cCreateTaskKVDataTable,
        cCreateTaskOutputTable,  // task_outputs table must be created before task_inputs
        cCreateTaskInputTable,
        cCreateTaskDependencyTable,
        cCreateTaskInstanceTable,
};

auto uuid_get_bytes(boost::uuids::uuid const& id) -> sql::bytes {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-cstyle-cast)
    return {(char const*)id.data(), id.size()};
    // NOLINTEND(cppcoreguidelines-pro-type-cstyle-cast)
}

auto task_state_to_string(spider::core::TaskState state) -> std::string {
    switch (state) {
        case spider::core::TaskState::Pending:
            return "pending";
        case spider::core::TaskState::Ready:
            return "ready";
        case spider::core::TaskState::Running:
            return "running";
        case spider::core::TaskState::Succeed:
            return "success";
        case spider::core::TaskState::Failed:
            return "fail";
        case spider::core::TaskState::Canceled:
            return "cancel";
        default:
            return "";
    }
}

auto string_to_task_state(std::string const& state) -> spider::core::TaskState {
    if (state == "pending") {
        return spider::core::TaskState::Pending;
    }
    if (state == "ready") {
        return spider::core::TaskState::Ready;
    }
    if (state == "running") {
        return spider::core::TaskState::Running;
    }
    if (state == "success") {
        return spider::core::TaskState::Succeed;
    }
    if (state == "fail") {
        return spider::core::TaskState::Failed;
    }
    if (state == "cancel") {
        return spider::core::TaskState::Canceled;
    }
    return spider::core::TaskState::Pending;
}
}  // namespace

auto MySqlMetadataStorage::connect(std::string const& url) -> StorageErr {
    if (nullptr == m_conn) {
        try {
            sql::Driver* driver = sql::mariadb::get_driver_instance();
            sql::Properties const properties;
            m_conn = driver->connect(sql::SQLString(url), properties);
            m_conn->setAutoCommit(false);
        } catch (sql::SQLException& e) {
            return StorageErr{StorageErrType::ConnectionErr, e.what()};
        }
    }
    return StorageErr{};
}

void MySqlMetadataStorage::close() {
    if (nullptr != m_conn) {
        m_conn->close();
        m_conn = nullptr;
    }
}

auto MySqlMetadataStorage::initialize() -> StorageErr {
    try {
        for (char const* create_table_str : cCreateStorage) {
            std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
            statement->executeUpdate(create_table_str);
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }

    m_conn->commit();
    return StorageErr{};
}

namespace {
// NOLINTBEGIN
auto read_id(std::istream* stream) -> boost::uuids::uuid {
    std::uint8_t id_bytes[16];
    stream->read((char*)id_bytes, 16);
    return {id_bytes};
}

// NOLINTEND
}  // namespace

auto MySqlMetadataStorage::add_driver(Driver const& driver) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("INSERT INTO `drivers` (`id`, `address`) VALUES (?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(driver.get_id());
        statement->setBytes(1, &id_bytes);
        statement->setString(2, driver.get_addr());
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_scheduler(Scheduler const& scheduler) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> driver_statement(
                m_conn->prepareStatement("INSERT INTO `drivers` (`id`, `address`) VALUES (?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(scheduler.get_id());
        driver_statement->setBytes(1, &id_bytes);
        driver_statement->setString(2, scheduler.get_addr());
        driver_statement->executeUpdate();
        std::unique_ptr<sql::PreparedStatement> scheduler_statement(m_conn->prepareStatement(
                "INSERT INTO `schedulers` (`id`, `port`, `state`) VALUES (?, ?, 'normal')"
        ));
        scheduler_statement->setBytes(1, &id_bytes);
        scheduler_statement->setInt(2, scheduler.get_port());
        scheduler_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_driver(boost::uuids::uuid id, std::string* addr) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("SELECT `address` FROM `drivers` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (0 == res->rowsCount()) {
            m_conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no driver with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *addr = res->getString(1).c_str();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_active_scheduler(std::vector<Scheduler>* schedulers) -> StorageErr {
    try {
        std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery(
                "SELECT `schedulers`.`id`, `address`, `port` FROM `schedulers` JOIN `drivers` ON "
                "`schedulers`.`id` = `drivers`.`id` WHERE `state` = 'normal'"
        ));
        while (res->next()) {
            boost::uuids::uuid const id = read_id(res->getBinaryStream(1));
            std::string const addr = res->getString(2).c_str();
            int const port = res->getInt(3);
            schedulers->emplace_back(id, addr, port);
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

void MySqlMetadataStorage::add_task(sql::bytes job_id, Task const& task) {
    // Add task
    std::unique_ptr<sql::PreparedStatement> task_statement(
            m_conn->prepareStatement("INSERT INTO `tasks` (`id`, `job_id`, `func_name`, `state`, "
                                     "`timeout`) VALUES (?, ?, ?, ?, ?)")
    );
    sql::bytes task_id_bytes = uuid_get_bytes(task.get_id());
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
    task_statement->setBytes(1, &task_id_bytes);
    task_statement->setBytes(2, &job_id);
    task_statement->setString(3, task.get_function_name());
    task_statement->setString(4, task_state_to_string(task.get_state()));
    task_statement->setFloat(5, task.get_timeout());
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
    task_statement->executeUpdate();

    // Add task inputs
    for (std::uint64_t i = 0; i < task.get_num_inputs(); ++i) {
        TaskInput const input = task.get_input(i);
        std::optional<std::tuple<boost::uuids::uuid, std::uint8_t>> const task_output
                = input.get_task_output();
        std::optional<boost::uuids::uuid> const data_id = input.get_data_id();
        std::optional<std::string> const& value = input.get_value();
        if (task_output.has_value()) {
            std::tuple<boost::uuids::uuid, std::uint8_t> const pair = task_output.value();
            std::unique_ptr<sql::PreparedStatement> input_statement(m_conn->prepareStatement(
                    "INSERT INTO `task_inputs` (`task_id`, `position`, `type`, `output_task_id`, "
                    "`output_task_position`) VALUES (?, ?, ?, ?, ?)"
            ));
            // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
            input_statement->setBytes(1, &task_id_bytes);
            input_statement->setUInt(2, i);
            input_statement->setString(3, input.get_type());
            sql::bytes task_output_id = uuid_get_bytes(std::get<0>(pair));
            input_statement->setBytes(4, &task_output_id);
            input_statement->setUInt(5, std::get<1>(pair));
            // NOLINTEND(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
            input_statement->executeUpdate();
        } else if (data_id.has_value()) {
            std::unique_ptr<sql::PreparedStatement> input_statement(
                    m_conn->prepareStatement("INSERT INTO `task_inputs` (`task_id`, `position`, "
                                             "`type`, `data_id`) VALUES (?, ?, ?, ?)")
            );
            input_statement->setBytes(1, &task_id_bytes);
            input_statement->setUInt(2, i);
            input_statement->setString(3, input.get_type());
            sql::bytes data_id_bytes = uuid_get_bytes(data_id.value());
            input_statement->setBytes(4, &data_id_bytes);
            input_statement->executeUpdate();
        } else if (value.has_value()) {
            std::unique_ptr<sql::PreparedStatement> input_statement(
                    m_conn->prepareStatement("INSERT INTO `task_inputs` (`task_id`, `position`, "
                                             "`type`, `value`) VALUES (?, ?, ?, ?)")
            );
            input_statement->setBytes(1, &task_id_bytes);
            input_statement->setUInt(2, i);
            input_statement->setString(3, input.get_type());
            input_statement->setString(4, value.value());
            input_statement->executeUpdate();
        }
    }

    // Add task outputs
    for (std::uint64_t i = 0; i < task.get_num_outputs(); i++) {
        TaskOutput const output = task.get_output(i);
        std::unique_ptr<sql::PreparedStatement> output_statement(m_conn->prepareStatement(
                "INSERT INTO `task_outputs` (`task_id`, `position`, `type`) VALUES (?, ?, ?)"
        ));
        output_statement->setBytes(1, &task_id_bytes);
        output_statement->setUInt(2, i);
        output_statement->setString(3, output.get_type());
        output_statement->executeUpdate();
    }
}

auto MySqlMetadataStorage::add_job(
        boost::uuids::uuid job_id,
        boost::uuids::uuid client_id,
        TaskGraph const& task_graph
) -> StorageErr {
    try {
        sql::bytes job_id_bytes = uuid_get_bytes(job_id);
        sql::bytes client_id_bytes = uuid_get_bytes(client_id);
        {
            std::unique_ptr<sql::PreparedStatement> statement{
                    m_conn->prepareStatement("INSERT INTO `jobs` (`id`, `client_id`) VALUES (?, ?)")
            };
            statement->setBytes(1, &job_id_bytes);
            statement->setBytes(2, &client_id_bytes);
            statement->executeUpdate();
        }

        // Tasks must be added in graph order to avoid the dangling reference.
        absl::flat_hash_set<boost::uuids::uuid> heads = task_graph.get_head_tasks();
        std::deque<boost::uuids::uuid> queue;
        // First go over all heads
        for (boost::uuids::uuid const task_id : heads) {
            std::optional<Task const*> const task_option = task_graph.get_task(task_id);
            if (!task_option.has_value()) {
                m_conn->rollback();
                return StorageErr{StorageErrType::KeyNotFoundErr, "Task graph inconsistent"};
            }
            Task const* task = task_option.value();
            this->add_task(job_id_bytes, *task);
            for (boost::uuids::uuid const id : task_graph.get_child_tasks(task_id)) {
                queue.push_back(id);
            }
        }
        // Then go over all tasks in queue
        while (!queue.empty()) {
            boost::uuids::uuid const task_id = queue.back();
            queue.pop_back();
            if (!heads.contains(task_id)) {
                heads.insert(task_id);
                std::optional<Task const*> const task_option = task_graph.get_task(task_id);
                if (!task_option.has_value()) {
                    m_conn->rollback();
                    return StorageErr{StorageErrType::KeyNotFoundErr, "Task graph inconsistent"};
                }
                Task const* task = task_option.value();
                this->add_task(job_id_bytes, *task);
                for (boost::uuids::uuid const id : task_graph.get_child_tasks(task_id)) {
                    queue.push_back(id);
                }
            }
        }

        // Add all dependencies
        for (std::pair<boost::uuids::uuid, boost::uuids::uuid> const& pair :
             task_graph.get_dependencies())
        {
            std::unique_ptr<sql::PreparedStatement> dep_statement{m_conn->prepareStatement(
                    "INSERT INTO `task_dependencies` (parent, child) VALUES (?, ?)"
            )};
            sql::bytes parent_id_bytes = uuid_get_bytes(pair.first);
            sql::bytes child_id_bytes = uuid_get_bytes(pair.second);
            dep_statement->setBytes(1, &parent_id_bytes);
            dep_statement->setBytes(2, &child_id_bytes);
            dep_statement->executeUpdate();
        }

        // Mark head tasks as ready
        for (boost::uuids::uuid const& task_id : task_graph.get_head_tasks()) {
            std::unique_ptr<sql::PreparedStatement> statement(
                    m_conn->prepareStatement("UPDATE `tasks` SET `state` = 'ready' WHERE `id` = ?")
            );
            sql::bytes task_id_bytes = uuid_get_bytes(task_id);
            statement->setBytes(1, &task_id_bytes);
            statement->executeUpdate();
        }

    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

namespace {

auto fetch_task(std::unique_ptr<sql::ResultSet> const& res) -> Task {
    boost::uuids::uuid const id = read_id(res->getBinaryStream("id"));
    std::string const function_name = res->getString("func_name").c_str();
    TaskState const state = string_to_task_state(res->getString("state").c_str());
    float const timeout = res->getFloat("timeout");
    return Task{id, function_name, state, timeout};
}

auto fetch_task_input(Task* task, std::unique_ptr<sql::ResultSet> const& res) {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    std::string const type = res->getString(3).c_str();
    if (!res->isNull(4)) {
        TaskInput input = TaskInput(read_id(res->getBinaryStream(4)), res->getUInt(5), type);
        if (!res->isNull(6)) {
            input.set_value(res->getString(6).c_str());
        }
        if (!res->isNull(7)) {
            input.set_data_id(read_id(res->getBinaryStream(7)));
        }
        task->add_input(input);
    } else if (!res->isNull(6)) {
        task->add_input(TaskInput(res->getString(6).c_str(), type));
    } else if (!res->isNull(7)) {
        task->add_input(TaskInput(read_id(res->getBinaryStream(7)), type));
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
}

auto fetch_task_output(Task* task, std::unique_ptr<sql::ResultSet> const& res) {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    std::string const type = res->getString(3).c_str();
    TaskOutput output{type};
    if (!res->isNull(4)) {
        output.set_value(res->getString(4).c_str());
    } else if (!res->isNull(5)) {
        output.set_data_id(read_id(res->getBinaryStream(5)));
    }
    task->add_output(output);
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
}

auto fetch_task_graph_task_input(TaskGraph* task_graph, std::unique_ptr<sql::ResultSet> const& res)
        -> bool {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    boost::uuids::uuid const task_id = read_id(res->getBinaryStream(1));
    std::string const type = res->getString(3).c_str();
    std::optional<Task*> task_option = task_graph->get_task(task_id);
    if (!task_option.has_value()) {
        return false;
    }
    Task* task = task_option.value();
    if (!res->isNull(4)) {
        TaskInput input = TaskInput(read_id(res->getBinaryStream(4)), res->getUInt(5), type);
        if (!res->isNull(6)) {
            input.set_value(res->getString(6).c_str());
        }
        if (!res->isNull(7)) {
            input.set_data_id(read_id(res->getBinaryStream(7)));
        }
        task->add_input(input);
    } else if (!res->isNull(6)) {
        task->add_input(TaskInput(res->getString(6).c_str(), type));
    } else if (!res->isNull(7)) {
        task->add_input(TaskInput(read_id(res->getBinaryStream(7)), type));
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    return true;
}

auto fetch_task_graph_task_output(TaskGraph* task_graph, std::unique_ptr<sql::ResultSet> const& res)
        -> bool {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    boost::uuids::uuid const task_id = read_id(res->getBinaryStream(1));
    std::optional<Task*> task_option = task_graph->get_task(task_id);
    if (!task_option.has_value()) {
        return false;
    }
    Task* task = task_option.value();
    std::string const type = res->getString(3).c_str();
    TaskOutput output{type};
    if (!res->isNull(4)) {
        output.set_value(res->getString(4).c_str());
    } else if (!res->isNull(5)) {
        output.set_data_id(read_id(res->getBinaryStream(5)));
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    task->add_output(output);
    return true;
}
}  // namespace

auto MySqlMetadataStorage::fetch_full_task(std::unique_ptr<sql::ResultSet> const& res) -> Task {
    Task task = fetch_task(res);
    boost::uuids::uuid const id = task.get_id();
    sql::bytes id_bytes = uuid_get_bytes(id);

    // Get task inputs
    std::unique_ptr<sql::PreparedStatement> input_statement{m_conn->prepareStatement(
            "SELECT `task_id`, `position`, `type`, `output_task_id`, `output_task_position`, "
            "`value`, `data_id` FROM `task_inputs` "
            "WHERE `task_id` = ? ORDER BY `position`"
    )};
    input_statement->setBytes(1, &id_bytes);
    std::unique_ptr<sql::ResultSet> const input_res{input_statement->executeQuery()};
    while (input_res->next()) {
        fetch_task_input(&task, input_res);
    }

    // Get task outputs
    std::unique_ptr<sql::PreparedStatement> output_statement{m_conn->prepareStatement(
            "SELECT `task_id`, `position`, `type`, `value`, `data_id` FROM `task_outputs` WHERE "
            "`task_id` = ? ORDER BY `position`"
    )};
    output_statement->setBytes(1, &id_bytes);
    std::unique_ptr<sql::ResultSet> const output_res{output_statement->executeQuery()};
    while (output_res->next()) {
        fetch_task_output(&task, output_res);
    }
    return task;
}

auto MySqlMetadataStorage::get_task_graph(boost::uuids::uuid id, TaskGraph* task_graph)
        -> StorageErr {
    try {
        // Get all tasks
        std::unique_ptr<sql::PreparedStatement> task_statement(
                m_conn->prepareStatement("SELECT `id`, `func_name`, `state`, `timeout` "
                                         "FROM `tasks` WHERE `job_id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        task_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const task_res(task_statement->executeQuery());
        if (task_res->rowsCount() == 0) {
            m_conn->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no task graph with id {}", boost::uuids::to_string(id))
            };
        }
        while (task_res->next()) {
            // get_task_graph has special optimization to get inputs and outputs in batch
            task_graph->add_task(fetch_task(task_res));
        }

        // Get inputs
        std::unique_ptr<sql::PreparedStatement> input_statement(m_conn->prepareStatement(
                "SELECT `t1`.`task_id`, `t1`.`position`, `t1`.`type`, `t1`.`output_task_id`, "
                "`t1`.`output_task_position`, `t1`.`value`, `t1`.`data_id` FROM `task_inputs` AS "
                "`t1` JOIN "
                "`tasks` "
                "ON `t1`.`task_id` = `tasks`.`id` WHERE `tasks`.`job_id` = ? ORDER BY "
                "`t1`.`task_id`, "
                "`t1`.`position`"
        ));
        input_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const input_res(input_statement->executeQuery());
        while (input_res->next()) {
            if (!fetch_task_graph_task_input(task_graph, input_res)) {
                m_conn->rollback();
                return StorageErr{StorageErrType::KeyNotFoundErr, "Task storage inconsistent"};
            }
        }

        // Get outputs
        std::unique_ptr<sql::PreparedStatement> output_statement(
                m_conn->prepareStatement("SELECT `t1`.`task_id`, `t1`.`position`, `t1`.`type`, "
                                         "`t1`.`value`, `t1`.`data_id` FROM "
                                         "`task_outputs` "
                                         "AS `t1` JOIN `tasks` ON `t1`.`task_id` = `tasks`.`id` "
                                         "WHERE `tasks`.`job_id` = ? ORDER BY "
                                         "`t1`.`task_id`, `t1`.`position`")
        );
        output_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const output_res(output_statement->executeQuery());
        while (output_res->next()) {
            if (!fetch_task_graph_task_output(task_graph, output_res)) {
                m_conn->rollback();
                return StorageErr{StorageErrType::KeyNotFoundErr, "Task storage inconsistent"};
            }
        }

        // Get dependencies
        std::unique_ptr<sql::PreparedStatement> dep_statement(
                m_conn->prepareStatement("SELECT `t1`.`parent`, `t1`.`child` FROM "
                                         "`task_dependencies` AS `t1` JOIN `tasks` ON "
                                         "`t1`.`parent` = `tasks`.`id` WHERE `tasks`.`job_id` = ?")
        );
        dep_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const dep_res(dep_statement->executeQuery());
        while (dep_res->next()) {
            task_graph->add_dependency(
                    read_id(dep_res->getBinaryStream(1)),
                    read_id(dep_res->getBinaryStream(2))
            );
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErKeyNotFound) {
            return StorageErr{StorageErrType::KeyNotFoundErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}
}  // namespace spider::core

namespace {

auto parse_timestamp(std::string const& timestamp
) -> std::optional<std::chrono::system_clock::time_point> {
    std::tm time_date{};
    std::stringstream ss{timestamp};
    ss >> std::get_time(&time_date, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) {
        return std::nullopt;
    }
    return std::chrono::system_clock::from_time_t(std::mktime(&time_date));
}

}  // namespace

namespace spider::core {

auto MySqlMetadataStorage::get_job_metadata(boost::uuids::uuid id, JobMetadata* job) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement{m_conn->prepareStatement(
                "SELECT `client_id`, `creation_time` FROM `jobs` WHERE `id` = ?"
        )};
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        if (0 == res->rowsCount()) {
            m_conn->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("No job with id {} ", boost::uuids::to_string(id))
            };
        }
        res->next();
        boost::uuids::uuid const client_id = read_id(res->getBinaryStream("client_id"));
        std::optional<std::chrono::system_clock::time_point> const optional_creation_time
                = parse_timestamp(res->getString("creation_time").c_str());
        if (false == optional_creation_time.has_value()) {
            m_conn->rollback();
            return StorageErr{
                    StorageErrType::OtherErr,
                    fmt::format(
                            "Cannot parse timestamp {}",
                            res->getString("creation_time").c_str()
                    )
            };
        }
        *job = JobMetadata{id, client_id, optional_creation_time.value()};
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_jobs_by_client_id(
        boost::uuids::uuid client_id,
        std::vector<boost::uuids::uuid>* job_ids
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement{
                m_conn->prepareStatement("SELECT `id` FROM `jobs` WHERE `client_id` = ?")
        };
        sql::bytes client_id_bytes = uuid_get_bytes(client_id);
        statement->setBytes(1, &client_id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        while (res->next()) {
            job_ids->emplace_back(read_id(res->getBinaryStream(1)));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::remove_job(boost::uuids::uuid id) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("DELETE FROM `jobs` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_child(boost::uuids::uuid parent_id, Task const& child)
        -> StorageErr {
    try {
        sql::bytes const job_id = uuid_get_bytes(child.get_id());
        this->add_task(job_id, child);

        // Add dependencies
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "INSERT INTO `task_dependencies` (`parent`, `child`) VALUES (?, ?)"
        ));
        sql::bytes parent_id_bytes = uuid_get_bytes(parent_id);
        sql::bytes child_id_bytes = uuid_get_bytes(child.get_id());
        statement->setBytes(1, &parent_id_bytes);
        statement->setBytes(2, &child_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task(boost::uuids::uuid id, Task* task) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("SELECT `id`, `func_name`, `state`, `timeout` "
                                         "FROM `tasks` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no task with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *task = fetch_full_task(res);
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_job_id(boost::uuids::uuid id, boost::uuids::uuid* job_id)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("SELECT `job_id` FROM `tasks` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no task with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *job_id = read_id(res->getBinaryStream("job_id"));
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_ready_tasks(std::vector<Task>* tasks) -> StorageErr {
    try {
        std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
        std::unique_ptr<sql::ResultSet> const res(
                statement->executeQuery("SELECT `id`, `func_name`, `state`, `timeout` "
                                        "FROM `tasks` WHERE `state` = 'ready'")
        );
        while (res->next()) {
            tasks->emplace_back(fetch_full_task(res));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::set_task_state(boost::uuids::uuid id, TaskState state) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("UPDATE `tasks` SET `state` = ? WHERE `id` = ?")
        );
        statement->setString(1, task_state_to_string(state));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(2, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErKeyNotFound) {
            return StorageErr{StorageErrType::KeyNotFoundErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_task_instance(TaskInstance const& instance) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> const statement(m_conn->prepareStatement(
                "INSERT INTO `task_instances` (`id`, `task_id`, `start_time`) "
                "VALUES(?, ?, CURRENT_TIMESTAMP())"
        ));
        sql::bytes id_bytes = uuid_get_bytes(instance.id);
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::task_finish(TaskInstance const& instance) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> const statement(m_conn->prepareStatement(
                "UPDATE `tasks` SET `instance_id` = ? WHERE `id` = ? AND `instance_id` is NULL"
        ));
        sql::bytes id_bytes = uuid_get_bytes(instance.id);
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_timeout(std::vector<TaskInstance>* tasks) -> StorageErr {
    try {
        std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery(
                "SELECT `t1`.`id`, `t1`.`task_id` FROM `task_instances` as `t1` JOIN `tasks` ON "
                "`t1`.`task_id` = "
                "`tasks`.`id` WHERE `tasks`.`timeout` > 0.0001 AND TIMESTAMPDIFF(MICROSECOND, "
                "`t1`.`start_time`, CURRENT_TIMESTAMP()) > `tasks`.`timeout` * 1000"
        ));
        while (res->next()) {
            tasks->emplace_back(read_id(res->getBinaryStream(1)), read_id(res->getBinaryStream(2)));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_child_tasks(boost::uuids::uuid id, std::vector<Task>* children)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` JOIN "
                "`task_dependencies` "
                "as `t2` WHERE `tasks`.`id` = `t2`.`child` AND `t2`.`parent` = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        while (res->next()) {
            children->emplace_back(fetch_full_task(res));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_parent_tasks(boost::uuids::uuid id, std::vector<Task>* tasks)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` JOIN "
                "`task_dependencies` "
                "as `t2` WHERE `tasks`.`id` = `t2`.`parent` AND `t2`.`child` = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        while (res->next()) {
            tasks->emplace_back(fetch_full_task(res));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::update_heartbeat(boost::uuids::uuid id) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "UPDATE `drivers` SET `heartbeat` = CURRENT_TIMESTAMP() WHERE `id` = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

namespace {
constexpr int cMillisecondToMicrosecond = 1000;
}  // namespace

auto MySqlMetadataStorage::heartbeat_timeout(double timeout, std::vector<boost::uuids::uuid>* ids)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "SELECT `id` FROM `drivers` WHERE TIMESTAMPDIFF(MICROSECOND, "
                "`heartbeat`, CURRENT_TIMESTAMP()) > ?"
        ));
        statement->setDouble(1, timeout * cMillisecondToMicrosecond);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        while (res->next()) {
            ids->emplace_back(read_id(res->getBinaryStream("id")));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_scheduler_state(boost::uuids::uuid id, std::string* state)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("SELECT `state` FROM `schedulers` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no scheduler with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *state = res->getString(1).c_str();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_scheduler_addr(boost::uuids::uuid id, std::string* addr, int* port)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> addr_statement(
                m_conn->prepareStatement("SELECT `address` FROM `drivers` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        addr_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> addr_res{addr_statement->executeQuery()};
        if (addr_res->rowsCount() == 0) {
            m_conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no driver with id {}", boost::uuids::to_string(id))
            };
        }
        std::unique_ptr<sql::PreparedStatement> port_statement(
                m_conn->prepareStatement("SELECT `port` FROM `schedulers` WHERE `id` = ?")
        );
        port_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> port_res{port_statement->executeQuery()};
        if (port_res->rowsCount() == 0) {
            m_conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no scheduler with id {}", boost::uuids::to_string(id))
            };
        }
        addr_res->next();
        *addr = addr_res->getString(1);
        port_res->next();
        *port = port_res->getInt(1);
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::set_scheduler_state(boost::uuids::uuid id, std::string const& state)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("UPDATE `schedulers` SET `state` = ? WHERE `id` = ?")
        );
        statement->setString(1, state);
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(2, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::connect(std::string const& url) -> StorageErr {
    if (nullptr == m_conn) {
        try {
            sql::Driver* driver = sql::mariadb::get_driver_instance();
            sql::Properties const properties;
            m_conn = driver->connect(sql::SQLString(url), properties);
            m_conn->setAutoCommit(false);
        } catch (sql::SQLException& e) {
            return StorageErr{StorageErrType::ConnectionErr, e.what()};
        }
    }
    return StorageErr{};
}

void MySqlDataStorage::close() {
    if (m_conn != nullptr) {
        m_conn->close();
        m_conn = nullptr;
    }
}

auto MySqlDataStorage::initialize() -> StorageErr {
    try {
        // Need to initialize metadata storage first so that foreign constraint is not voilated
        for (char const* create_table_str : cCreateStorage) {
            std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
            statement->executeUpdate(create_table_str);
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }

    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_driver_data(boost::uuids::uuid const driver_id, Data const& data)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "INSERT INTO `data` (`id`, `value`, `hard_locality`) VALUES(?, ?, ?)"
        ));
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(1, &id_bytes);
        statement->setString(2, data.get_value());
        statement->setBoolean(3, data.is_hard_locality());
        statement->executeUpdate();

        for (std::string const& addr : data.get_locality()) {
            std::unique_ptr<sql::PreparedStatement> locality_statement(
                    m_conn->prepareStatement("INSERT INTO `data_locality` (`id`, "
                                             "`address`) VALUES (?, ?)")
            );
            locality_statement->setBytes(1, &id_bytes);
            locality_statement->setString(2, addr);
            locality_statement->executeUpdate();
        }
        std::unique_ptr<sql::PreparedStatement> driver_ref_statement(m_conn->prepareStatement(
                "INSERT INTO `data_ref_driver` (`id`, `driver_id`) VALUES(?, ?)"
        ));
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        driver_ref_statement->setBytes(1, &id_bytes);
        driver_ref_statement->setBytes(2, &driver_id_bytes);
        driver_ref_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_data(boost::uuids::uuid const task_id, Data const& data)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "INSERT INTO `data` (`id`, `value`, `hard_locality`) VALUES(?, ?, ?)"
        ));
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(1, &id_bytes);
        statement->setString(2, data.get_value());
        statement->setBoolean(3, data.is_hard_locality());
        statement->executeUpdate();

        for (std::string const& addr : data.get_locality()) {
            std::unique_ptr<sql::PreparedStatement> locality_statement(
                    m_conn->prepareStatement("INSERT INTO `data_locality` (`id`, "
                                             "`address`) VALUES (?, ?)")
            );
            locality_statement->setBytes(1, &id_bytes);
            locality_statement->setString(2, addr);
            locality_statement->executeUpdate();
        }
        std::unique_ptr<sql::PreparedStatement> task_ref_statement(m_conn->prepareStatement(
                "INSERT INTO `data_ref_task` (`id`, `task_id`) VALUES(?, ?)"
        ));
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        task_ref_statement->setBytes(1, &id_bytes);
        task_ref_statement->setBytes(2, &task_id_bytes);
        task_ref_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_data(boost::uuids::uuid id, Data* data) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("SELECT `id`, `value`, `hard_locality` "
                                         "FROM `data` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no data with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *data = Data{id, res->getString(2).c_str()};
        data->set_hard_locality(res->getBoolean(3));

        std::unique_ptr<sql::PreparedStatement> locality_statement(
                m_conn->prepareStatement("SELECT `address` FROM `data_locality` WHERE `id` = ?")
        );
        locality_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const locality_res(locality_statement->executeQuery());
        std::vector<std::string> locality;
        while (locality_res->next()) {
            locality.emplace_back(locality_res->getString(1));
        }
        if (!locality.empty()) {
            data->set_locality(locality);
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_data(boost::uuids::uuid id) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("DELETE FROM `data` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("INSERT INTO `data_ref_task` (`id`, "
                                         "`task_id`) VALUES(?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("DELETE FROM `data_ref_task` WHERE "
                                         "`id` = ? AND `task_id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("INSERT INTO `data_ref_driver` (`id`, "
                                         "`driver_id`) VALUES(?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("DELETE FROM `data_ref_driver` "
                                         "WHERE `id` = ? AND `driver_id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_dangling_data() -> StorageErr {
    try {
        std::unique_ptr<sql::Statement> statement{m_conn->createStatement()};
        statement->execute("DELETE FROM `data` WHERE `id` NOT IN (SELECT driver_ref.`id` FROM "
                           "`data_ref_driver` driver_ref) AND `id` NOT IN (SELECT task_ref.`id` "
                           "FROM `data_ref_task` task_ref)");
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_client_kv_data(KeyValueData const& data) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "INSERT INTO `client_kv_data` (`kv_key`, `value`, `client_id`) VALUES(?, ?, ?)"
        ));
        statement->setString(1, data.get_key());
        statement->setString(2, data.get_value());
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(3, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_kv_data(KeyValueData const& data) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "INSERT INTO `task_kv_data` (`kv_key`, `value`, `task_id`) VALUES(?, ?, ?)"
        ));
        statement->setString(1, data.get_key());
        statement->setString(2, data.get_value());
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(3, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_client_kv_data(
        boost::uuids::uuid const& client_id,
        std::string const& key,
        std::string* value
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "SELECT `value` "
                "FROM `client_kv_data` WHERE `client_id` = ? AND `kv_key` = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(client_id);
        statement->setBytes(1, &id_bytes);
        statement->setString(2, key);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format(
                            "no data for client {} with key {}",
                            boost::uuids::to_string(client_id),
                            key
                    )
            };
        }
        res->next();
        *value = res->getString(1);
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_task_kv_data(
        boost::uuids::uuid const& task_id,
        std::string const& key,
        std::string* value
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("SELECT `value` "
                                         "FROM `task_kv_data` WHERE `task_id` = ? AND `kv_key` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(1, &id_bytes);
        statement->setString(2, key);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format(
                            "no data for task {} with key {}",
                            boost::uuids::to_string(task_id),
                            key
                    )
            };
        }
        res->next();
        *value = res->getString(1);
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    m_conn->commit();
    return StorageErr{};
}

}  // namespace spider::core
