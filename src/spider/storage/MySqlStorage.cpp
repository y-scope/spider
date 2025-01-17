#include "MySqlStorage.hpp"

#include <algorithm>
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
#include <variant>
#include <vector>

#include <absl/container/flat_hash_set.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>
#include <mariadb/conncpp/CArray.hpp>
#include <mariadb/conncpp/Exception.hpp>
#include <mariadb/conncpp/jdbccompat.hpp>
#include <mariadb/conncpp/PreparedStatement.hpp>
#include <mariadb/conncpp/ResultSet.hpp>
#include <mariadb/conncpp/Statement.hpp>
#include <mariadb/conncpp/Types.hpp>
#include <spdlog/spdlog.h>

#include "../core/Data.hpp"
#include "../core/Driver.hpp"
#include "../core/Error.hpp"
#include "../core/JobMetadata.hpp"
#include "../core/KeyValueData.hpp"
#include "../core/Task.hpp"
#include "../core/TaskGraph.hpp"
#include "MySqlConnection.hpp"

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
    `heartbeat` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
))";

char const* const cCreateSchedulerTable = R"(CREATE TABLE IF NOT EXISTS `schedulers` (
    `id` BINARY(16) NOT NULL,
    `address` VARCHAR(40) NOT NULL,
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
    `retry` INT UNSIGNED DEFAULT 0,
    `instance_id` BINARY(16),
    CONSTRAINT `task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
))";

char const* const cCreateInputTaskTable = R"(CREATE TABLE IF NOT EXISTS input_tasks (
    `job_id` BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    CONSTRAINT `input_task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `input_task_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`job_id`, `position`),
    PRIMARY KEY (`task_id`)
))";

char const* const cCreateOutputTaskTable = R"(CREATE TABLE IF NOT EXISTS output_tasks (
    `job_id` BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    CONSTRAINT `output_task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `output_task_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`job_id`, `position`),
    PRIMARY KEY (`task_id`)
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

std::array<char const* const, 16> const cCreateStorage = {
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
        cCreateInputTaskTable,
        cCreateOutputTaskTable,
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

auto MySqlMetadataStorage::initialize() -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        for (char const* create_table_str : cCreateStorage) {
            std::unique_ptr<sql::Statement> statement(conn->createStatement());
            statement->executeUpdate(create_table_str);
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }

    conn->commit();
    return StorageErr{};
}

namespace {
// NOLINTBEGIN
auto read_id(std::istream* stream) -> boost::uuids::uuid {
    std::uint8_t id_bytes[16];
    stream->read((char*)id_bytes, 16);
    return {id_bytes};
}

auto get_sql_string(sql::SQLString const& str) -> std::string {
    std::string result{str.c_str(), str.size()};
    return result;
}

// NOLINTEND
}  // namespace

auto MySqlMetadataStorage::add_driver(Driver const& driver) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("INSERT INTO `drivers` (`id`) VALUES (?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(driver.get_id());
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_scheduler(Scheduler const& scheduler) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> driver_statement(
                conn->prepareStatement("INSERT INTO `drivers` (`id`) VALUES (?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(scheduler.get_id());
        driver_statement->setBytes(1, &id_bytes);
        driver_statement->executeUpdate();
        std::unique_ptr<sql::PreparedStatement> scheduler_statement(conn->prepareStatement(
                "INSERT INTO `schedulers` (`id`, `address`, `port`, `state`) "
                "VALUES (?, ?, ?, 'normal')"
        ));
        scheduler_statement->setBytes(1, &id_bytes);
        scheduler_statement->setString(2, scheduler.get_addr());
        scheduler_statement->setInt(3, scheduler.get_port());
        scheduler_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_active_scheduler(std::vector<Scheduler>* schedulers) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::Statement> statement(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery(
                "SELECT `schedulers`.`id`, `address`, `port` FROM `schedulers` JOIN `drivers` ON "
                "`schedulers`.`id` = `drivers`.`id` WHERE `state` = 'normal'"
        ));
        while (res->next()) {
            boost::uuids::uuid const id = read_id(res->getBinaryStream(1));
            std::string const addr = get_sql_string(res->getString(2));
            int const port = res->getInt(3);
            schedulers->emplace_back(id, addr, port);
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

void MySqlMetadataStorage::add_task(MySqlConnection& conn, sql::bytes job_id, Task const& task) {
    // Add task
    std::unique_ptr<sql::PreparedStatement> task_statement(
            conn->prepareStatement("INSERT INTO `tasks` (`id`, `job_id`, `func_name`, `state`, "
                                   "`timeout`, `max_retry`) VALUES (?, ?, ?, ?, ?, ?)")
    );
    sql::bytes task_id_bytes = uuid_get_bytes(task.get_id());
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
    task_statement->setBytes(1, &task_id_bytes);
    task_statement->setBytes(2, &job_id);
    task_statement->setString(3, task.get_function_name());
    task_statement->setString(4, task_state_to_string(task.get_state()));
    task_statement->setFloat(5, task.get_timeout());
    task_statement->setUInt(6, task.get_max_retries());
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
            std::unique_ptr<sql::PreparedStatement> input_statement(conn->prepareStatement(
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
                    conn->prepareStatement("INSERT INTO `task_inputs` (`task_id`, `position`, "
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
                    conn->prepareStatement("INSERT INTO `task_inputs` (`task_id`, `position`, "
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
        std::unique_ptr<sql::PreparedStatement> output_statement(conn->prepareStatement(
                "INSERT INTO `task_outputs` (`task_id`, `position`, `type`) VALUES (?, ?, ?)"
        ));
        output_statement->setBytes(1, &task_id_bytes);
        output_statement->setUInt(2, i);
        output_statement->setString(3, output.get_type());
        output_statement->executeUpdate();
    }
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
auto MySqlMetadataStorage::add_job(
        boost::uuids::uuid job_id,
        boost::uuids::uuid client_id,
        TaskGraph const& task_graph
) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        sql::bytes job_id_bytes = uuid_get_bytes(job_id);
        sql::bytes client_id_bytes = uuid_get_bytes(client_id);
        {
            std::unique_ptr<sql::PreparedStatement> statement{
                    conn->prepareStatement("INSERT INTO `jobs` (`id`, `client_id`) VALUES (?, ?)")
            };
            statement->setBytes(1, &job_id_bytes);
            statement->setBytes(2, &client_id_bytes);
            statement->executeUpdate();
        }

        // Tasks must be added in graph order to avoid the dangling reference.
        std::vector<boost::uuids::uuid> const& input_task_ids = task_graph.get_input_tasks();
        absl::flat_hash_set<boost::uuids::uuid> heads;
        for (boost::uuids::uuid const task_id : input_task_ids) {
            heads.insert(task_id);
        }
        std::deque<boost::uuids::uuid> queue;
        // First go over all heads
        for (boost::uuids::uuid const task_id : heads) {
            std::optional<Task const*> const task_option = task_graph.get_task(task_id);
            if (!task_option.has_value()) {
                conn->rollback();
                return StorageErr{
                        StorageErrType::KeyNotFoundErr,
                        "Task graph inconsistent: head task not found"
                };
            }
            Task const* task = task_option.value();
            add_task(conn, job_id_bytes, *task);
            for (boost::uuids::uuid const id : task_graph.get_child_tasks(task_id)) {
                std::vector<boost::uuids::uuid> const parents = task_graph.get_parent_tasks(id);
                if (std::ranges::all_of(parents, [&](boost::uuids::uuid const& parent) {
                        return heads.contains(parent);
                    }))
                {
                    queue.push_back(id);
                }
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
                    conn->rollback();
                    return StorageErr{StorageErrType::KeyNotFoundErr, "Task graph inconsistent"};
                }
                Task const* task = task_option.value();
                add_task(conn, job_id_bytes, *task);
                for (boost::uuids::uuid const id : task_graph.get_child_tasks(task_id)) {
                    std::vector<boost::uuids::uuid> const parents = task_graph.get_parent_tasks(id);
                    if (std::ranges::all_of(parents, [&](boost::uuids::uuid const& parent) {
                            return heads.contains(parent);
                        }))
                    {
                        queue.push_back(id);
                    }
                }
            }
        }

        // Add all dependencies
        for (std::pair<boost::uuids::uuid, boost::uuids::uuid> const& pair :
             task_graph.get_dependencies())
        {
            std::unique_ptr<sql::PreparedStatement> dep_statement{conn->prepareStatement(
                    "INSERT INTO `task_dependencies` (parent, child) VALUES (?, ?)"
            )};
            sql::bytes parent_id_bytes = uuid_get_bytes(pair.first);
            sql::bytes child_id_bytes = uuid_get_bytes(pair.second);
            dep_statement->setBytes(1, &parent_id_bytes);
            dep_statement->setBytes(2, &child_id_bytes);
            dep_statement->executeUpdate();
        }

        // Add input tasks
        for (size_t i = 0; i < input_task_ids.size(); i++) {
            std::unique_ptr<sql::PreparedStatement> input_statement{conn->prepareStatement(
                    "INSERT INTO `input_tasks` (`job_id`, `task_id`, `position`) VALUES (?, ?, ?)"
            )};
            input_statement->setBytes(1, &job_id_bytes);
            sql::bytes task_id_bytes = uuid_get_bytes(input_task_ids[i]);
            input_statement->setBytes(2, &task_id_bytes);
            input_statement->setUInt(3, i);
            input_statement->executeUpdate();
        }
        // Add output tasks
        std::vector<boost::uuids::uuid> const& output_task_ids = task_graph.get_output_tasks();
        for (size_t i = 0; i < output_task_ids.size(); i++) {
            std::unique_ptr<sql::PreparedStatement> output_statement{conn->prepareStatement(
                    "INSERT INTO `output_tasks` (`job_id`, `task_id`, `position`) VALUES (?, ?, ?)"
            )};
            output_statement->setBytes(1, &job_id_bytes);
            sql::bytes task_id_bytes = uuid_get_bytes(output_task_ids[i]);
            output_statement->setBytes(2, &task_id_bytes);
            output_statement->setUInt(3, i);
            output_statement->executeUpdate();
        }

        // Mark head tasks as ready
        for (boost::uuids::uuid const& task_id : task_graph.get_input_tasks()) {
            std::unique_ptr<sql::PreparedStatement> statement(
                    conn->prepareStatement("UPDATE `tasks` SET `state` = 'ready' WHERE `id` = ?")
            );
            sql::bytes task_id_bytes = uuid_get_bytes(task_id);
            statement->setBytes(1, &task_id_bytes);
            statement->executeUpdate();
        }

    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

// NOLINTEND(readability-function-cognitive-complexity)

namespace {

auto fetch_task(std::unique_ptr<sql::ResultSet> const& res) -> Task {
    boost::uuids::uuid const id = read_id(res->getBinaryStream("id"));
    std::string const function_name = get_sql_string(res->getString("func_name"));
    TaskState const state = string_to_task_state(get_sql_string(res->getString("state")));
    float const timeout = res->getFloat("timeout");
    return Task{id, function_name, state, timeout};
}

auto fetch_task_input(Task* task, std::unique_ptr<sql::ResultSet> const& res) {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    std::string const type = get_sql_string(res->getString(3));
    if (!res->isNull(4)) {
        TaskInput input = TaskInput(read_id(res->getBinaryStream(4)), res->getUInt(5), type);
        if (!res->isNull(6)) {
            input.set_value(get_sql_string(res->getString(6)));
        }
        if (!res->isNull(7)) {
            input.set_data_id(read_id(res->getBinaryStream(7)));
        }
        task->add_input(input);
    } else if (!res->isNull(6)) {
        task->add_input(TaskInput(get_sql_string(res->getString(6)), type));
    } else if (!res->isNull(7)) {
        task->add_input(TaskInput(read_id(res->getBinaryStream(7))));
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
}

auto fetch_task_output(Task* task, std::unique_ptr<sql::ResultSet> const& res) {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    std::string const type = get_sql_string(res->getString(3));
    TaskOutput output{type};
    if (!res->isNull(4)) {
        output.set_value(get_sql_string(res->getString(4)));
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
    std::string const type = get_sql_string(res->getString(3));
    std::optional<Task*> task_option = task_graph->get_task(task_id);
    if (!task_option.has_value()) {
        return false;
    }
    Task* task = task_option.value();
    if (!res->isNull(4)) {
        TaskInput input = TaskInput(read_id(res->getBinaryStream(4)), res->getUInt(5), type);
        if (!res->isNull(6)) {
            input.set_value(get_sql_string(res->getString(6)));
        }
        if (!res->isNull(7)) {
            input.set_data_id(read_id(res->getBinaryStream(7)));
        }
        task->add_input(input);
    } else if (!res->isNull(6)) {
        task->add_input(TaskInput(get_sql_string(res->getString(6)), type));
    } else if (!res->isNull(7)) {
        task->add_input(TaskInput(read_id(res->getBinaryStream(7))));
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
    std::string const type = get_sql_string(res->getString(3));
    TaskOutput output{type};
    if (!res->isNull(4)) {
        output.set_value(get_sql_string(res->getString(4)));
    } else if (!res->isNull(5)) {
        output.set_data_id(read_id(res->getBinaryStream(5)));
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    task->add_output(output);
    return true;
}
}  // namespace

auto MySqlMetadataStorage::fetch_full_task(
        MySqlConnection& conn,
        std::unique_ptr<sql::ResultSet> const& res
) -> Task {
    Task task = fetch_task(res);
    boost::uuids::uuid const id = task.get_id();
    sql::bytes id_bytes = uuid_get_bytes(id);

    // Get task inputs
    std::unique_ptr<sql::PreparedStatement> input_statement{conn->prepareStatement(
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
    std::unique_ptr<sql::PreparedStatement> output_statement{conn->prepareStatement(
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
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        // Get all tasks
        std::unique_ptr<sql::PreparedStatement> task_statement(
                conn->prepareStatement("SELECT `id`, `func_name`, `state`, `timeout` "
                                       "FROM `tasks` WHERE `job_id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        task_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const task_res(task_statement->executeQuery());
        if (task_res->rowsCount() == 0) {
            conn->commit();
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
        std::unique_ptr<sql::PreparedStatement> input_statement(conn->prepareStatement(
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
                conn->rollback();
                return StorageErr{StorageErrType::KeyNotFoundErr, "Task storage inconsistent"};
            }
        }

        // Get outputs
        std::unique_ptr<sql::PreparedStatement> output_statement(
                conn->prepareStatement("SELECT `t1`.`task_id`, `t1`.`position`, `t1`.`type`, "
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
                conn->rollback();
                return StorageErr{StorageErrType::KeyNotFoundErr, "Task storage inconsistent"};
            }
        }

        // Get dependencies
        std::unique_ptr<sql::PreparedStatement> dep_statement(
                conn->prepareStatement("SELECT `t1`.`parent`, `t1`.`child` FROM "
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

        // Get input tasks
        std::unique_ptr<sql::PreparedStatement> input_task_statement(
                conn->prepareStatement("SELECT `task_id`, `position` FROM `input_tasks` WHERE "
                                       "`job_id` = ? ORDER BY `position`")
        );
        input_task_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const input_task_res(input_task_statement->executeQuery());
        while (input_task_res->next()) {
            task_graph->add_input_task(read_id(input_task_res->getBinaryStream(1)));
        }
        // Get output tasks
        std::unique_ptr<sql::PreparedStatement> output_task_statement(
                conn->prepareStatement("SELECT `task_id`, `position` FROM `output_tasks` WHERE "
                                       "`job_id` = ? ORDER BY `position`")
        );
        output_task_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const output_task_res(output_task_statement->executeQuery()
        );
        while (output_task_res->next()) {
            task_graph->add_output_task(read_id(output_task_res->getBinaryStream(1)));
        }

    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErKeyNotFound) {
            return StorageErr{StorageErrType::KeyNotFoundErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
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
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement{conn->prepareStatement(
                "SELECT `client_id`, `creation_time` FROM `jobs` WHERE `id` = ?"
        )};
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        if (0 == res->rowsCount()) {
            conn->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("No job with id {} ", boost::uuids::to_string(id))
            };
        }
        res->next();
        boost::uuids::uuid const client_id = read_id(res->getBinaryStream("client_id"));
        std::optional<std::chrono::system_clock::time_point> const optional_creation_time
                = parse_timestamp(get_sql_string(res->getString("creation_time")));
        if (false == optional_creation_time.has_value()) {
            conn->rollback();
            return StorageErr{
                    StorageErrType::OtherErr,
                    fmt::format(
                            "Cannot parse timestamp {}",
                            get_sql_string(res->getString("creation_time"))
                    )
            };
        }
        *job = JobMetadata{id, client_id, optional_creation_time.value()};
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_job_complete(boost::uuids::uuid const id, bool* complete)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> const statement{
                conn->prepareStatement("SELECT `state` FROM `tasks` WHERE `job_id` = ? AND "
                                       "`state` NOT IN ('success', 'cancel', 'fail') ")
        };
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        *complete = 0 == res->rowsCount();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_job_status(boost::uuids::uuid const id, JobStatus* status)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> const running_statement{
                conn->prepareStatement("SELECT `state` FROM `tasks` WHERE `job_id` = ? AND "
                                       "`state` NOT IN ('success', 'cancel', 'fail') ")
        };
        sql::bytes id_bytes = uuid_get_bytes(id);
        running_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const running_res{running_statement->executeQuery()};
        if (running_res->rowsCount() > 0) {
            *status = JobStatus::Running;
            conn->commit();
            return StorageErr{};
        }
        std::unique_ptr<sql::PreparedStatement> failed_statement{conn->prepareStatement(
                "SELECT `state` FROM `tasks` WHERE `job_id` = ? AND `state` = 'fail'"
        )};
        failed_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const failed_res{failed_statement->executeQuery()};
        if (failed_res->rowsCount() > 0) {
            *status = JobStatus::Failed;
            conn->commit();
            return StorageErr{};
        }
        std::unique_ptr<sql::PreparedStatement> canceled_statement{conn->prepareStatement(
                "SELECT `state` FROM `tasks` WHERE `job_id` = ? AND `state` = 'cancel'"
        )};
        canceled_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const canceled_res{canceled_statement->executeQuery()};
        if (canceled_res->rowsCount() > 0) {
            *status = JobStatus::Cancelled;
            conn->commit();
            return StorageErr{};
        }
        *status = JobStatus::Succeeded;
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_job_output_tasks(
        boost::uuids::uuid const id,
        std::vector<boost::uuids::uuid>* task_ids
) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        task_ids->clear();
        std::unique_ptr<sql::PreparedStatement> statement{conn->prepareStatement(
                "SELECT `task_id` FROM `output_tasks` WHERE `job_id` = ? ORDER BY `position`"
        )};
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        while (res->next()) {
            task_ids->emplace_back(read_id(res->getBinaryStream(1)));
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_jobs_by_client_id(
        boost::uuids::uuid client_id,
        std::vector<boost::uuids::uuid>* job_ids
) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement{
                conn->prepareStatement("SELECT `id` FROM `jobs` WHERE `client_id` = ?")
        };
        sql::bytes client_id_bytes = uuid_get_bytes(client_id);
        statement->setBytes(1, &client_id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        while (res->next()) {
            job_ids->emplace_back(read_id(res->getBinaryStream(1)));
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::remove_job(boost::uuids::uuid id) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("DELETE FROM `jobs` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::reset_job(boost::uuids::uuid const id) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        // Check for retry count on all tasks
        std::unique_ptr<sql::PreparedStatement> retry_statement(conn->prepareStatement(
                "SELECT `id` FROM `tasks` WHERE `job_id` = ? AND `retry` >= `max_retry`"
        ));
        sql::bytes job_id_bytes = uuid_get_bytes(id);
        retry_statement->setBytes(1, &job_id_bytes);
        std::unique_ptr<sql::ResultSet> const res(retry_statement->executeQuery());
        if (res->rowsCount() > 0) {
            conn->commit();
            return StorageErr{StorageErrType::Success, "Some tasks have reached max retry count"};
        }
        // Increment the retry count for all tasks
        std::unique_ptr<sql::PreparedStatement> increment_statement(conn->prepareStatement(
                "UPDATE `tasks` SET `retry` = `retry` + 1 WHERE `job_id` = ?"
        ));
        increment_statement->setBytes(1, &job_id_bytes);
        increment_statement->executeUpdate();
        // Reset states for all tasks. Head tasks should be ready and other tasks should be pending
        std::unique_ptr<sql::PreparedStatement> state_statement(conn->prepareStatement(
                "UPDATE `tasks` SET `state` = IF(`id` NOT IN (SELECT `task_id` FROM `task_inputs` "
                "WHERE `task_id` IN (SELECT `id` FROM `tasks` WHERE `job_id` = ?) AND "
                "`output_task_id` IS NOT NULL), 'ready', 'pending') WHERE job_id = ?"
        ));
        state_statement->setBytes(1, &job_id_bytes);
        state_statement->setBytes(2, &job_id_bytes);
        state_statement->executeUpdate();
        // Clear outputs for all tasks
        std::unique_ptr<sql::PreparedStatement> output_statement(conn->prepareStatement(
                "UPDATE `task_outputs` SET `value` = NULL, `data_id` = NULL "
                "WHERE `task_id` IN (SELECT `id` FROM `tasks` WHERE `job_id` = ?)"
        ));
        output_statement->setBytes(1, &job_id_bytes);
        output_statement->executeUpdate();
        // Clear inputs for non-head tasks
        std::unique_ptr<sql::PreparedStatement> input_statement(conn->prepareStatement(
                "UPDATE `task_inputs` SET `value` = NULL, `data_id` = NULL "
                "WHERE `task_id` IN (SELECT `id` FROM `tasks` WHERE `job_id` = ?) "
                "AND `output_task_id` IS NOT NULL"
        ));
        input_statement->setBytes(1, &job_id_bytes);
        input_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_child(boost::uuids::uuid parent_id, Task const& child)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        sql::bytes const job_id = uuid_get_bytes(child.get_id());
        add_task(conn, job_id, child);

        // Add dependencies
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "INSERT INTO `task_dependencies` (`parent`, `child`) VALUES (?, ?)"
        ));
        sql::bytes parent_id_bytes = uuid_get_bytes(parent_id);
        sql::bytes child_id_bytes = uuid_get_bytes(child.get_id());
        statement->setBytes(1, &parent_id_bytes);
        statement->setBytes(2, &child_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task(boost::uuids::uuid id, Task* task) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("SELECT `id`, `func_name`, `state`, `timeout` "
                                       "FROM `tasks` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            conn->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no task with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *task = fetch_full_task(conn, res);
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_job_id(boost::uuids::uuid id, boost::uuids::uuid* job_id)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("SELECT `job_id` FROM `tasks` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            conn->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no task with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *job_id = read_id(res->getBinaryStream("job_id"));
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_ready_tasks(std::vector<Task>* tasks) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        // Get all ready tasks from job that has not failed or cancelled
        std::unique_ptr<sql::Statement> statement(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery(
                "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` WHERE `state` = 'ready' "
                "AND `job_id` NOT IN (SELECT `job_id` FROM `tasks` WHERE `state` = 'fail' OR "
                "`state` = 'cancel')"
        ));
        while (res->next()) {
            tasks->emplace_back(fetch_full_task(conn, res));
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::set_task_state(boost::uuids::uuid id, TaskState state) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("UPDATE `tasks` SET `state` = ? WHERE `id` = ?")
        );
        statement->setString(1, task_state_to_string(state));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(2, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErKeyNotFound) {
            return StorageErr{StorageErrType::KeyNotFoundErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::set_task_running(boost::uuids::uuid id) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "UPDATE `tasks` SET `state` = 'running' WHERE `id` = ? AND `state` = 'ready'"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        int32_t const update_count = statement->executeUpdate();
        if (update_count == 0) {
            conn->rollback();
            return StorageErr{StorageErrType::KeyNotFoundErr, "Task not ready"};
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_task_instance(TaskInstance const& instance) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> const statement(conn->prepareStatement(
                "INSERT INTO `task_instances` (`id`, `task_id`, `start_time`) "
                "VALUES(?, ?, CURRENT_TIMESTAMP())"
        ));
        sql::bytes id_bytes = uuid_get_bytes(instance.id);
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::task_finish(
        TaskInstance const& instance,
        std::vector<TaskOutput> const& outputs
) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        // Try to submit task instance
        std::unique_ptr<sql::PreparedStatement> const statement(conn->prepareStatement(
                "UPDATE `tasks` SET `instance_id` = ?, `state` = 'success' WHERE `id` = ? AND "
                "`instance_id` is NULL AND `state` = 'running'"
        ));
        sql::bytes id_bytes = uuid_get_bytes(instance.id);
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        int32_t const update_count = statement->executeUpdate();
        if (update_count == 0) {
            conn->commit();
            return StorageErr{};
        }

        // Update task outputs
        std::unique_ptr<sql::PreparedStatement> output_statement(conn->prepareStatement(
                "UPDATE `task_outputs` SET `value` = ?, `data_id` = ? WHERE `task_id` = ? AND "
                "`position` = ?"
        ));
        for (size_t i = 0; i < outputs.size(); ++i) {
            TaskOutput const& output = outputs[i];
            std::optional<std::string> const& value = output.get_value();
            if (value.has_value()) {
                output_statement->setString(1, value.value());
            } else {
                output_statement->setNull(1, sql::DataType::VARCHAR);
            }
            std::optional<boost::uuids::uuid> const& data_id = output.get_data_id();
            if (data_id.has_value()) {
                sql::bytes data_id_bytes = uuid_get_bytes(data_id.value());
                output_statement->setBytes(2, &data_id_bytes);
            } else {
                output_statement->setNull(2, sql::DataType::BINARY);
            }
            output_statement->setBytes(3, &task_id_bytes);
            output_statement->setUInt(4, i);
            output_statement->executeUpdate();
        }

        // Update task inputs
        std::unique_ptr<sql::PreparedStatement> input_statement(conn->prepareStatement(
                "UPDATE `task_inputs` SET `value` = ?, `data_id` = ? WHERE `output_task_id` = ? "
                "AND `output_task_position` = ?"
        ));
        for (size_t i = 0; i < outputs.size(); ++i) {
            TaskOutput const& output = outputs[i];
            std::optional<std::string> const& value = output.get_value();
            if (value.has_value()) {
                input_statement->setString(1, value.value());
            } else {
                input_statement->setNull(1, sql::DataType::VARCHAR);
            }
            std::optional<boost::uuids::uuid> const& data_id = output.get_data_id();
            if (data_id.has_value()) {
                sql::bytes data_id_bytes = uuid_get_bytes(data_id.value());
                input_statement->setBytes(2, &data_id_bytes);
            } else {
                input_statement->setNull(2, sql::DataType::BINARY);
            }
            input_statement->setBytes(3, &task_id_bytes);
            input_statement->setUInt(4, i);
            input_statement->executeUpdate();
        }

        // Set task states to ready if all inputs are available
        std::unique_ptr<sql::PreparedStatement> ready_statement(conn->prepareStatement(
                "UPDATE `tasks` SET `state` = 'ready' WHERE `id` IN (SELECT `task_id` FROM "
                "`task_inputs` WHERE `output_task_id` = ?) AND `state` = 'pending' AND NOT EXISTS "
                "(SELECT `task_id` FROM `task_inputs` WHERE `task_id` IN (SELECT `task_id` FROM "
                "`task_inputs` WHERE `output_task_id` = ?) AND `value` IS NULL AND `data_id` IS "
                "NULL)"
        ));
        ready_statement->setBytes(1, &task_id_bytes);
        ready_statement->setBytes(2, &task_id_bytes);
        ready_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::task_fail(TaskInstance const& instance, std::string const& /*error*/)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        // Remove task instance
        std::unique_ptr<sql::PreparedStatement> const statement(
                conn->prepareStatement("DELETE FROM `task_instances` WHERE `id` = ?")
        );
        sql::bytes instance_id_bytes = uuid_get_bytes(instance.id);
        statement->setBytes(1, &instance_id_bytes);
        statement->executeUpdate();

        // Get number of remaining instances
        std::unique_ptr<sql::PreparedStatement> const count_statement(
                conn->prepareStatement("SELECT COUNT(*) FROM `task_instances` WHERE `task_id` = ?")
        );
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        count_statement->setBytes(1, &task_id_bytes);
        std::unique_ptr<sql::ResultSet> const count_res{count_statement->executeQuery()};
        count_res->next();
        int32_t const count = count_res->getInt(1);
        if (count == 0) {
            // Set the task fail if the last task instance fails
            std::unique_ptr<sql::PreparedStatement> const task_statement(
                    conn->prepareStatement("UPDATE `tasks` SET `state` = 'fail' WHERE `id` = ?")
            );
            task_statement->setBytes(1, &task_id_bytes);
            task_statement->executeUpdate();
        }
    } catch (sql::SQLException& e) {
        spdlog::error("Task fail error: {}", e.what());
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_timeout(std::vector<TaskInstance>* tasks) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::Statement> statement(conn->createStatement());
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
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_child_tasks(boost::uuids::uuid id, std::vector<Task>* children)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` JOIN "
                "`task_dependencies` "
                "as `t2` WHERE `tasks`.`id` = `t2`.`child` AND `t2`.`parent` = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        while (res->next()) {
            children->emplace_back(fetch_full_task(conn, res));
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_parent_tasks(boost::uuids::uuid id, std::vector<Task>* tasks)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` JOIN "
                "`task_dependencies` "
                "as `t2` WHERE `tasks`.`id` = `t2`.`parent` AND `t2`.`child` = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        while (res->next()) {
            tasks->emplace_back(fetch_full_task(conn, res));
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::update_heartbeat(boost::uuids::uuid id) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "UPDATE `drivers` SET `heartbeat` = CURRENT_TIMESTAMP() WHERE `id` = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

namespace {
constexpr int cMillisecondToMicrosecond = 1000;
}  // namespace

auto MySqlMetadataStorage::heartbeat_timeout(double timeout, std::vector<boost::uuids::uuid>* ids)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "SELECT `id` FROM `drivers` WHERE TIMESTAMPDIFF(MICROSECOND, "
                "`heartbeat`, CURRENT_TIMESTAMP()) > ?"
        ));
        statement->setDouble(1, timeout * cMillisecondToMicrosecond);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        while (res->next()) {
            ids->emplace_back(read_id(res->getBinaryStream("id")));
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_scheduler_state(boost::uuids::uuid id, std::string* state)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("SELECT `state` FROM `schedulers` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no scheduler with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *state = get_sql_string(res->getString(1));
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_scheduler_addr(boost::uuids::uuid id, std::string* addr, int* port)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("SELECT `address`, `port` FROM `schedulers` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res{statement->executeQuery()};
        if (res->rowsCount() == 0) {
            conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no scheduler with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *addr = get_sql_string(res->getString(1));
        *port = res->getInt(2);
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::set_scheduler_state(boost::uuids::uuid id, std::string const& state)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("UPDATE `schedulers` SET `state` = ? WHERE `id` = ?")
        );
        statement->setString(1, state);
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(2, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::initialize() -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
        if (std::holds_alternative<StorageErr>(conn_result)) {
            return std::get<StorageErr>(conn_result);
        }
        auto& conn = std::get<MySqlConnection>(conn_result);
        // Need to initialize metadata storage first so that foreign constraint is not voilated
        for (char const* create_table_str : cCreateStorage) {
            std::unique_ptr<sql::Statement> statement(conn->createStatement());
            statement->executeUpdate(create_table_str);
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }

    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_driver_data(boost::uuids::uuid const driver_id, Data const& data)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "INSERT INTO `data` (`id`, `value`, `hard_locality`) VALUES(?, ?, ?)"
        ));
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(1, &id_bytes);
        statement->setString(2, data.get_value());
        statement->setBoolean(3, data.is_hard_locality());
        statement->executeUpdate();

        for (std::string const& addr : data.get_locality()) {
            std::unique_ptr<sql::PreparedStatement> locality_statement(
                    conn->prepareStatement("INSERT INTO `data_locality` (`id`, "
                                           "`address`) VALUES (?, ?)")
            );
            locality_statement->setBytes(1, &id_bytes);
            locality_statement->setString(2, addr);
            locality_statement->executeUpdate();
        }
        std::unique_ptr<sql::PreparedStatement> driver_ref_statement(conn->prepareStatement(
                "INSERT INTO `data_ref_driver` (`id`, `driver_id`) VALUES(?, ?)"
        ));
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        driver_ref_statement->setBytes(1, &id_bytes);
        driver_ref_statement->setBytes(2, &driver_id_bytes);
        driver_ref_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_data(boost::uuids::uuid const task_id, Data const& data)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "INSERT INTO `data` (`id`, `value`, `hard_locality`) VALUES(?, ?, ?)"
        ));
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(1, &id_bytes);
        statement->setString(2, data.get_value());
        statement->setBoolean(3, data.is_hard_locality());
        statement->executeUpdate();

        for (std::string const& addr : data.get_locality()) {
            std::unique_ptr<sql::PreparedStatement> locality_statement(
                    conn->prepareStatement("INSERT INTO `data_locality` (`id`, "
                                           "`address`) VALUES (?, ?)")
            );
            locality_statement->setBytes(1, &id_bytes);
            locality_statement->setString(2, addr);
            locality_statement->executeUpdate();
        }
        std::unique_ptr<sql::PreparedStatement> task_ref_statement(
                conn->prepareStatement("INSERT INTO `data_ref_task` (`id`, `task_id`) VALUES(?, ?)")
        );
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        task_ref_statement->setBytes(1, &id_bytes);
        task_ref_statement->setBytes(2, &task_id_bytes);
        task_ref_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_data(boost::uuids::uuid id, Data* data) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("SELECT `id`, `value`, `hard_locality` "
                                       "FROM `data` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            conn->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no data with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *data = Data{id, get_sql_string(res->getString(2))};
        data->set_hard_locality(res->getBoolean(3));

        std::unique_ptr<sql::PreparedStatement> locality_statement(
                conn->prepareStatement("SELECT `address` FROM `data_locality` WHERE `id` = ?")
        );
        locality_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const locality_res(locality_statement->executeQuery());
        std::vector<std::string> locality;
        while (locality_res->next()) {
            locality.emplace_back(get_sql_string(locality_res->getString(1)));
        }
        if (!locality.empty()) {
            data->set_locality(locality);
        }
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::set_data_locality(Data const& data) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> const delete_statement(
                conn->prepareStatement("DELETE FROM `data_locality` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        delete_statement->setBytes(1, &id_bytes);
        delete_statement->executeUpdate();
        std::unique_ptr<sql::PreparedStatement> const insert_statement(
                conn->prepareStatement("INSERT INTO `data_locality` (`id`, `address`) VALUES(?, ?)")
        );
        for (std::string const& addr : data.get_locality()) {
            insert_statement->setBytes(1, &id_bytes);
            insert_statement->setString(2, addr);
            insert_statement->executeUpdate();
        }
        std::unique_ptr<sql::PreparedStatement> const hard_locality_statement(
                conn->prepareStatement("UPDATE `data` SET `hard_locality` = ? WHERE `id` = ?")
        );
        hard_locality_statement->setBoolean(1, data.is_hard_locality());
        hard_locality_statement->setBytes(2, &id_bytes);
        hard_locality_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_data(boost::uuids::uuid id) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("DELETE FROM `data` WHERE `id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("INSERT INTO `data_ref_task` (`id`, "
                                       "`task_id`) VALUES(?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("DELETE FROM `data_ref_task` WHERE "
                                       "`id` = ? AND `task_id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("INSERT INTO `data_ref_driver` (`id`, "
                                       "`driver_id`) VALUES(?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id)
        -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("DELETE FROM `data_ref_driver` "
                                       "WHERE `id` = ? AND `driver_id` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_dangling_data() -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::Statement> statement{conn->createStatement()};
        statement->execute("DELETE FROM `data` WHERE `id` NOT IN (SELECT driver_ref.`id` FROM "
                           "`data_ref_driver` driver_ref) AND `id` NOT IN (SELECT task_ref.`id` "
                           "FROM `data_ref_task` task_ref)");
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_client_kv_data(KeyValueData const& data) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "INSERT INTO `client_kv_data` (`kv_key`, `value`, `client_id`) VALUES(?, ?, ?)"
        ));
        statement->setString(1, data.get_key());
        statement->setString(2, data.get_value());
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(3, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_kv_data(KeyValueData const& data) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "INSERT INTO `task_kv_data` (`kv_key`, `value`, `task_id`) VALUES(?, ?, ?)"
        ));
        statement->setString(1, data.get_key());
        statement->setString(2, data.get_value());
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(3, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        conn->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_client_kv_data(
        boost::uuids::uuid const& client_id,
        std::string const& key,
        std::string* value
) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(conn->prepareStatement(
                "SELECT `value` "
                "FROM `client_kv_data` WHERE `client_id` = ? AND `kv_key` = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(client_id);
        statement->setBytes(1, &id_bytes);
        statement->setString(2, key);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            conn->rollback();
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
        *value = get_sql_string(res->getString(1));
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_task_kv_data(
        boost::uuids::uuid const& task_id,
        std::string const& key,
        std::string* value
) -> StorageErr {
    std::variant<MySqlConnection, StorageErr> conn_result = MySqlConnection::create(m_url);
    if (std::holds_alternative<StorageErr>(conn_result)) {
        return std::get<StorageErr>(conn_result);
    }
    auto& conn = std::get<MySqlConnection>(conn_result);
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                conn->prepareStatement("SELECT `value` "
                                       "FROM `task_kv_data` WHERE `task_id` = ? AND `kv_key` = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(1, &id_bytes);
        statement->setString(2, key);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            conn->rollback();
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
        *value = get_sql_string(res->getString(1));
    } catch (sql::SQLException& e) {
        conn->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    conn->commit();
    return StorageErr{};
}

}  // namespace spider::core
