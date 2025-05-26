#include "MySqlStorage.hpp"

#include <algorithm>
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

#include <absl/container/flat_hash_map.h>
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

#include <spider/core/Data.hpp>
#include <spider/core/Driver.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/JobMetadata.hpp>
#include <spider/core/KeyValueData.hpp>
#include <spider/core/Task.hpp>
#include <spider/core/TaskGraph.hpp>
#include <spider/storage/JobSubmissionBatch.hpp>
#include <spider/storage/mysql/mysql_stmt.hpp>
#include <spider/storage/mysql/MySqlConnection.hpp>
#include <spider/storage/mysql/MySqlJobSubmissionBatch.hpp>
#include <spider/storage/StorageConnection.hpp>

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
    ErDeadLock = 1213,
};

namespace spider::core {
namespace {
auto uuid_get_bytes(boost::uuids::uuid const& id) -> sql::bytes {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-cstyle-cast)
    return {(char const*)id.data(), id.size()};
    // NOLINTEND(cppcoreguidelines-pro-type-cstyle-cast)
}

// NOLINTBEGIN
auto read_id(std::istream* stream) -> boost::uuids::uuid {
    std::uint8_t id_bytes[16];
    stream->read((char*)id_bytes, 16);
    return {id_bytes};
}

auto string_get_bytes(std::string const& str) -> sql::bytes {
    return {str.c_str(), str.size()};
}

auto get_sql_string(sql::SQLString const& str) -> std::string {
    std::string result{str.c_str(), str.size()};
    return result;
}

// NOLINTEND

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

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast)
auto MySqlMetadataStorage::initialize(StorageConnection& conn) -> StorageErr {
    try {
        for (std::string const& create_table_str : mysql::cCreateStorage) {
            std::unique_ptr<sql::Statement> statement(
                    static_cast<MySqlConnection&>(conn)->createStatement()
            );
            statement->executeUpdate(create_table_str);
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }

    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_driver(StorageConnection& conn, Driver const& driver) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `drivers` (`id`) VALUES (?)"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(driver.get_id());
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_scheduler(StorageConnection& conn, Scheduler const& scheduler)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> driver_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `drivers` (`id`) VALUES (?)"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(scheduler.get_id());
        driver_statement->setBytes(1, &id_bytes);
        driver_statement->executeUpdate();
        std::unique_ptr<sql::PreparedStatement> scheduler_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `schedulers` (`id`, `address`, `port`) VALUES (?, ?, ?)"
                )
        );
        scheduler_statement->setBytes(1, &id_bytes);
        scheduler_statement->setString(2, scheduler.get_addr());
        scheduler_statement->setInt(3, scheduler.get_port());
        scheduler_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_active_scheduler(
        StorageConnection& conn,
        std::vector<Scheduler>* schedulers
) -> StorageErr {
    try {
        std::unique_ptr<sql::Statement> statement(
                static_cast<MySqlConnection&>(conn)->createStatement()
        );
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery(
                "SELECT `schedulers`.`id`, `address`, `port` FROM `schedulers` JOIN `drivers` ON "
                "`schedulers`.`id` = `drivers`.`id`"
        ));
        while (res->next()) {
            boost::uuids::uuid const id = read_id(res->getBinaryStream(1));
            std::string const addr = get_sql_string(res->getString(2));
            int const port = res->getInt(3);
            schedulers->emplace_back(id, addr, port);
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

void MySqlMetadataStorage::add_task(
        MySqlConnection& conn,
        sql::bytes job_id,
        Task const& task,
        std::optional<TaskState> const& state
) {
    // Add task
    std::unique_ptr<sql::PreparedStatement> task_statement(
            conn->prepareStatement(mysql::cInsertTask)
    );
    sql::bytes task_id_bytes = uuid_get_bytes(task.get_id());
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
    task_statement->setBytes(1, &task_id_bytes);
    task_statement->setBytes(2, &job_id);
    task_statement->setString(3, task.get_function_name());
    if (state.has_value()) {
        task_statement->setString(4, task_state_to_string(state.value()));
    } else {
        task_statement->setString(4, task_state_to_string(task.get_state()));
    }
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
            std::unique_ptr<sql::PreparedStatement> input_statement(
                    conn->prepareStatement(mysql::cInsertTaskInputOutput)
            );
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
                    conn->prepareStatement(mysql::cInsertTaskInputData)
            );
            input_statement->setBytes(1, &task_id_bytes);
            input_statement->setUInt(2, i);
            input_statement->setString(3, input.get_type());
            sql::bytes data_id_bytes = uuid_get_bytes(data_id.value());
            input_statement->setBytes(4, &data_id_bytes);
            input_statement->executeUpdate();
        } else if (value.has_value()) {
            std::unique_ptr<sql::PreparedStatement> input_statement(
                    conn->prepareStatement(mysql::cInsertTaskInputValue)
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
        std::unique_ptr<sql::PreparedStatement> output_statement(
                conn->prepareStatement(mysql::cInsertTaskOutput)
        );
        output_statement->setBytes(1, &task_id_bytes);
        output_statement->setUInt(2, i);
        output_statement->setString(3, output.get_type());
        output_statement->executeUpdate();
    }
}

void MySqlMetadataStorage::add_task_batch(
        MySqlJobSubmissionBatch& batch,
        sql::bytes job_id,
        Task const& task,
        std::optional<TaskState> const& state
) {
    // Add task
    sql::PreparedStatement& task_statement = batch.get_task_stmt();
    sql::bytes task_id_bytes = uuid_get_bytes(task.get_id());
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
    task_statement.setBytes(1, &task_id_bytes);
    task_statement.setBytes(2, &job_id);
    task_statement.setString(3, task.get_function_name());
    if (state.has_value()) {
        task_statement.setString(4, task_state_to_string(state.value()));
    } else {
        task_statement.setString(4, task_state_to_string(task.get_state()));
    }
    task_statement.setFloat(5, task.get_timeout());
    task_statement.setUInt(6, task.get_max_retries());
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
    task_statement.addBatch();

    // Add task inputs
    for (std::uint64_t i = 0; i < task.get_num_inputs(); ++i) {
        TaskInput const input = task.get_input(i);
        std::optional<std::tuple<boost::uuids::uuid, std::uint8_t>> const task_output
                = input.get_task_output();
        std::optional<boost::uuids::uuid> const data_id = input.get_data_id();
        std::optional<std::string> const& value = input.get_value();
        if (task_output.has_value()) {
            std::tuple<boost::uuids::uuid, std::uint8_t> const pair = task_output.value();
            sql::PreparedStatement& input_statement = batch.get_task_input_output_stmt();
            // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
            input_statement.setBytes(1, &task_id_bytes);
            input_statement.setUInt(2, i);
            input_statement.setString(3, input.get_type());
            sql::bytes task_output_id = uuid_get_bytes(std::get<0>(pair));
            input_statement.setBytes(4, &task_output_id);
            input_statement.setUInt(5, std::get<1>(pair));
            // NOLINTEND(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
            input_statement.addBatch();
        } else if (data_id.has_value()) {
            sql::PreparedStatement& input_statement = batch.get_task_input_data_stmt();
            input_statement.setBytes(1, &task_id_bytes);
            input_statement.setUInt(2, i);
            input_statement.setString(3, input.get_type());
            sql::bytes data_id_bytes = uuid_get_bytes(data_id.value());
            input_statement.setBytes(4, &data_id_bytes);
            input_statement.addBatch();
        } else if (value.has_value()) {
            sql::PreparedStatement& input_statement = batch.get_task_input_value_stmt();
            input_statement.setBytes(1, &task_id_bytes);
            input_statement.setUInt(2, i);
            input_statement.setString(3, input.get_type());
            input_statement.setString(4, value.value());
            input_statement.addBatch();
        }
    }

    // Add task outputs
    for (std::uint64_t i = 0; i < task.get_num_outputs(); i++) {
        TaskOutput const output = task.get_output(i);
        sql::PreparedStatement& output_statement = batch.get_task_output_stmt();
        output_statement.setBytes(1, &task_id_bytes);
        output_statement.setUInt(2, i);
        output_statement.setString(3, output.get_type());
        output_statement.addBatch();
    }
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
auto MySqlMetadataStorage::add_job(
        StorageConnection& conn,
        boost::uuids::uuid job_id,
        boost::uuids::uuid client_id,
        TaskGraph const& task_graph
) -> StorageErr {
    try {
        sql::bytes job_id_bytes = uuid_get_bytes(job_id);
        sql::bytes client_id_bytes = uuid_get_bytes(client_id);
        {
            std::unique_ptr<sql::PreparedStatement> statement{
                    static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertJob)
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
                static_cast<MySqlConnection&>(conn)->rollback();
                return StorageErr{
                        StorageErrType::KeyNotFoundErr,
                        "Task graph inconsistent: head task not found"
                };
            }
            Task const* task = task_option.value();
            add_task(static_cast<MySqlConnection&>(conn), job_id_bytes, *task, TaskState::Ready);
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
                    static_cast<MySqlConnection&>(conn)->rollback();
                    return StorageErr{StorageErrType::KeyNotFoundErr, "Task graph inconsistent"};
                }
                Task const* task = task_option.value();
                add_task(static_cast<MySqlConnection&>(conn), job_id_bytes, *task, std::nullopt);
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
            std::unique_ptr<sql::PreparedStatement> dep_statement{
                    static_cast<MySqlConnection&>(conn)->prepareStatement(
                            mysql::cInsertTaskDependency
                    )
            };
            sql::bytes parent_id_bytes = uuid_get_bytes(pair.first);
            sql::bytes child_id_bytes = uuid_get_bytes(pair.second);
            dep_statement->setBytes(1, &parent_id_bytes);
            dep_statement->setBytes(2, &child_id_bytes);
            dep_statement->executeUpdate();
        }

        // Add input tasks
        for (size_t i = 0; i < input_task_ids.size(); i++) {
            std::unique_ptr<sql::PreparedStatement> input_statement{
                    static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertInputTask)
            };
            input_statement->setBytes(1, &job_id_bytes);
            sql::bytes task_id_bytes = uuid_get_bytes(input_task_ids[i]);
            input_statement->setBytes(2, &task_id_bytes);
            input_statement->setUInt(3, i);
            input_statement->executeUpdate();
        }
        // Add output tasks
        std::vector<boost::uuids::uuid> const& output_task_ids = task_graph.get_output_tasks();
        for (size_t i = 0; i < output_task_ids.size(); i++) {
            std::unique_ptr<sql::PreparedStatement> output_statement{
                    static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertOutputTask)
            };
            output_statement->setBytes(1, &job_id_bytes);
            sql::bytes task_id_bytes = uuid_get_bytes(output_task_ids[i]);
            output_statement->setBytes(2, &task_id_bytes);
            output_statement->setUInt(3, i);
            output_statement->executeUpdate();
        }

    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_job_batch(
        StorageConnection& conn,
        JobSubmissionBatch& batch,
        boost::uuids::uuid job_id,
        boost::uuids::uuid client_id,
        TaskGraph const& task_graph
) -> StorageErr {
    try {
        sql::bytes job_id_bytes = uuid_get_bytes(job_id);
        sql::bytes client_id_bytes = uuid_get_bytes(client_id);
        {
            sql::PreparedStatement& statement
                    = static_cast<MySqlJobSubmissionBatch&>(batch).get_job_stmt();
            statement.setBytes(1, &job_id_bytes);
            statement.setBytes(2, &client_id_bytes);
            statement.addBatch();
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
                static_cast<MySqlConnection&>(conn)->rollback();
                return StorageErr{
                        StorageErrType::KeyNotFoundErr,
                        "Task graph inconsistent: head task not found"
                };
            }
            Task const* task = task_option.value();
            add_task_batch(
                    static_cast<MySqlJobSubmissionBatch&>(batch),
                    job_id_bytes,
                    *task,
                    TaskState::Ready
            );
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
                    static_cast<MySqlConnection&>(conn)->rollback();
                    return StorageErr{StorageErrType::KeyNotFoundErr, "Task graph inconsistent"};
                }
                Task const* task = task_option.value();
                add_task_batch(
                        static_cast<MySqlJobSubmissionBatch&>(batch),
                        job_id_bytes,
                        *task,
                        std::nullopt
                );
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
            sql::PreparedStatement& dep_statement
                    = static_cast<MySqlJobSubmissionBatch&>(batch).get_task_dependency_stmt();
            sql::bytes parent_id_bytes = uuid_get_bytes(pair.first);
            sql::bytes child_id_bytes = uuid_get_bytes(pair.second);
            dep_statement.setBytes(1, &parent_id_bytes);
            dep_statement.setBytes(2, &child_id_bytes);
            dep_statement.addBatch();
        }

        // Add input tasks
        for (size_t i = 0; i < input_task_ids.size(); i++) {
            sql::PreparedStatement& input_statement
                    = static_cast<MySqlJobSubmissionBatch&>(batch).get_input_task_stmt();
            input_statement.setBytes(1, &job_id_bytes);
            sql::bytes task_id_bytes = uuid_get_bytes(input_task_ids[i]);
            input_statement.setBytes(2, &task_id_bytes);
            input_statement.setUInt(3, i);
            input_statement.addBatch();
        }
        // Add output tasks
        std::vector<boost::uuids::uuid> const& output_task_ids = task_graph.get_output_tasks();
        for (size_t i = 0; i < output_task_ids.size(); i++) {
            sql::PreparedStatement& output_statement
                    = static_cast<MySqlJobSubmissionBatch&>(batch).get_output_task_stmt();
            output_statement.setBytes(1, &job_id_bytes);
            sql::bytes task_id_bytes = uuid_get_bytes(output_task_ids[i]);
            output_statement.setBytes(2, &task_id_bytes);
            output_statement.setUInt(3, i);
            output_statement.addBatch();
        }

    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
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
            "`value`, `data_id` FROM `task_inputs` WHERE `task_id` = ? ORDER BY `position`"
    )};
    input_statement->setBytes(1, &id_bytes);
    std::unique_ptr<sql::ResultSet> const input_res{input_statement->executeQuery()};
    while (input_res->next()) {
        fetch_task_input(&task, input_res);
    }

    // Get task outputs
    std::unique_ptr<sql::PreparedStatement> output_statement{conn->prepareStatement(
            "SELECT `task_id`, `position`, `type`, `value`, `data_id` FROM "
            "`task_outputs` WHERE `task_id` = ? ORDER BY `position`"
    )};
    output_statement->setBytes(1, &id_bytes);
    std::unique_ptr<sql::ResultSet> const output_res{output_statement->executeQuery()};
    while (output_res->next()) {
        fetch_task_output(&task, output_res);
    }
    return task;
}

auto MySqlMetadataStorage::get_task_graph(
        StorageConnection& conn,
        boost::uuids::uuid id,
        TaskGraph* task_graph
) -> StorageErr {
    try {
        // Get all tasks
        std::unique_ptr<sql::PreparedStatement> task_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` WHERE `job_id` "
                        "= ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        task_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const task_res(task_statement->executeQuery());
        if (task_res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->commit();
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
        std::unique_ptr<sql::PreparedStatement> input_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `t1`.`task_id`, `t1`.`position`, `t1`.`type`, "
                        "`t1`.`output_task_id`, `t1`.`output_task_position`, `t1`.`value`, "
                        "`t1`.`data_id` FROM `task_inputs` AS `t1` JOIN `tasks` ON `t1`.`task_id` "
                        "= `tasks`.`id` WHERE `tasks`.`job_id` = ? ORDER BY `t1`.`task_id`, "
                        "`t1`.`position`"
                )
        );
        input_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const input_res(input_statement->executeQuery());
        while (input_res->next()) {
            if (!fetch_task_graph_task_input(task_graph, input_res)) {
                static_cast<MySqlConnection&>(conn)->rollback();
                return StorageErr{StorageErrType::KeyNotFoundErr, "Task storage inconsistent"};
            }
        }

        // Get outputs
        std::unique_ptr<sql::PreparedStatement> output_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `t1`.`task_id`, `t1`.`position`, `t1`.`type`, `t1`.`value`, "
                        "`t1`.`data_id` FROM `task_outputs` AS `t1` JOIN `tasks` ON `t1`.`task_id` "
                        "= `tasks`.`id` WHERE `tasks`.`job_id` = ? ORDER BY `t1`.`task_id`, "
                        "`t1`.`position`"
                )
        );
        output_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const output_res(output_statement->executeQuery());
        while (output_res->next()) {
            if (!fetch_task_graph_task_output(task_graph, output_res)) {
                static_cast<MySqlConnection&>(conn)->rollback();
                return StorageErr{StorageErrType::KeyNotFoundErr, "Task storage inconsistent"};
            }
        }

        // Get dependencies
        std::unique_ptr<sql::PreparedStatement> dep_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `t1`.`parent`, `t1`.`child` FROM `task_dependencies` AS `t1` JOIN "
                        "`tasks` ON `t1`.`parent` = `tasks`.`id` WHERE `tasks`.`job_id` = ?"
                )
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
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `task_id`, `position` FROM `input_tasks` WHERE `job_id` = ? ORDER "
                        "BY `position`"
                )
        );
        input_task_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const input_task_res(input_task_statement->executeQuery());
        while (input_task_res->next()) {
            task_graph->add_input_task(read_id(input_task_res->getBinaryStream(1)));
        }
        // Get output tasks
        std::unique_ptr<sql::PreparedStatement> output_task_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `task_id`, `position` FROM `output_tasks` WHERE `job_id` = ? ORDER "
                        "BY `position`"
                )
        );
        output_task_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const output_task_res(
                output_task_statement->executeQuery()
        );
        while (output_task_res->next()) {
            task_graph->add_output_task(read_id(output_task_res->getBinaryStream(1)));
        }

    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErKeyNotFound) {
            return StorageErr{StorageErrType::KeyNotFoundErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}
}  // namespace spider::core

namespace {
auto parse_timestamp(std::string const& timestamp)
        -> std::optional<std::chrono::system_clock::time_point> {
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
auto MySqlMetadataStorage::get_job_metadata(
        StorageConnection& conn,
        boost::uuids::uuid id,
        JobMetadata* job
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement{
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `client_id`, `creation_time` FROM `jobs` WHERE `id` = ?"
                )
        };
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        if (0 == res->rowsCount()) {
            static_cast<MySqlConnection&>(conn)->commit();
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
            static_cast<MySqlConnection&>(conn)->rollback();
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
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_job_complete(
        StorageConnection& conn,
        boost::uuids::uuid const id,
        bool* complete
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> const statement{
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `state` FROM `jobs` WHERE `id` = ?"
                )
        };
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("No job with id {} ", boost::uuids::to_string(id))
            };
        }
        res->next();
        std::string const state = get_sql_string(res->getString("state"));
        *complete = ("running" != state);
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_job_status(
        StorageConnection& conn,
        boost::uuids::uuid const id,
        JobStatus* status
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> const job_statement{
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `state` FROM `jobs` WHERE `id` = ?"
                )
        };
        sql::bytes id_bytes = uuid_get_bytes(id);
        job_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{job_statement->executeQuery()};
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("No job with id {} ", boost::uuids::to_string(id))
            };
        }
        res->next();
        std::string const state = get_sql_string(res->getString("state"));
        if ("running" == state) {
            *status = JobStatus::Running;
        } else if ("success" == state) {
            *status = JobStatus::Succeeded;
        } else if ("fail" == state) {
            *status = JobStatus::Failed;
        } else if ("cancel" == state) {
            *status = JobStatus::Cancelled;
        } else {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{StorageErrType::OtherErr, "Unknown job status"};
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_job_output_tasks(
        StorageConnection& conn,
        boost::uuids::uuid const id,
        std::vector<boost::uuids::uuid>* task_ids
) -> StorageErr {
    try {
        task_ids->clear();
        std::unique_ptr<sql::PreparedStatement> statement{
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `task_id` FROM `output_tasks` WHERE `job_id` = ? ORDER BY "
                        "`position`"
                )
        };
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        while (res->next()) {
            task_ids->emplace_back(read_id(res->getBinaryStream(1)));
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_jobs_by_client_id(
        StorageConnection& conn,
        boost::uuids::uuid client_id,
        std::vector<boost::uuids::uuid>* job_ids
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement{
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `id` FROM `jobs` WHERE `client_id` = ?"
                )
        };
        sql::bytes client_id_bytes = uuid_get_bytes(client_id);
        statement->setBytes(1, &client_id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        while (res->next()) {
            job_ids->emplace_back(read_id(res->getBinaryStream(1)));
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::cancel_job(StorageConnection& conn, boost::uuids::uuid const id)
        -> StorageErr {
    try {
        // Set all pending/ready/running tasks from the job to cancelled
        std::unique_ptr<sql::PreparedStatement> task_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `state` = 'cancel' WHERE `job_id` = ? AND "
                        "`state` IN ('pending', 'ready', 'running')"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        task_statement->setBytes(1, &id_bytes);
        task_statement->executeUpdate();
        // Set job state to cancelled
        std::unique_ptr<sql::PreparedStatement> job_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `jobs` SET `state` = 'cancel' WHERE `id` = ?"
                )
        );
        job_statement->setBytes(1, &id_bytes);
        job_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::cancel_job_by_task(
        StorageConnection& conn,
        boost::uuids::uuid id,
        std::string const& message
) -> StorageErr {
    try {
        // Get job id
        sql::bytes task_id_bytes = uuid_get_bytes(id);
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `job_id`, `func_name` FROM `tasks` WHERE `id` = ?"
                )
        );
        statement->setBytes(1, &task_id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{StorageErrType::KeyNotFoundErr, "No task with id"};
        }
        res->next();
        boost::uuids::uuid const job_id = read_id(res->getBinaryStream("job_id"));
        sql::bytes job_id_bytes = uuid_get_bytes(job_id);
        std::string const function_name = get_sql_string(res->getString("func_name"));
        // Set all pending/ready/running tasks from the job to cancelled
        std::unique_ptr<sql::PreparedStatement> task_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `state` = 'cancel' WHERE `job_id` = ? AND "
                        "`state` IN ('pending', 'ready', 'running')"
                )
        );
        task_statement->setBytes(1, &job_id_bytes);
        task_statement->executeUpdate();
        // Set job state to cancelled
        std::unique_ptr<sql::PreparedStatement> job_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `jobs` SET `state` = 'cancel' WHERE `id` = ?"
                )
        );
        job_statement->setBytes(1, &job_id_bytes);
        job_statement->executeUpdate();
        // Set the cancel message
        std::unique_ptr<sql::PreparedStatement> message_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `job_errors` (`job_id`, `func_name`, `message`) VALUES (?, ?, "
                        "?) "
                )
        );
        message_statement->setBytes(1, &job_id_bytes);
        message_statement->setString(2, function_name);
        message_statement->setString(3, message);
        message_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_job_message(
        StorageConnection& conn,
        boost::uuids::uuid const id,
        std::string* function_name,
        std::string* message
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement{
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `func_name`, `message` FROM `job_errors` WHERE `job_id` = ?"
                )
        };
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res{statement->executeQuery()};
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->commit();
            return StorageErr{StorageErrType::KeyNotFoundErr, "No messages found"};
        }
        res->next();
        *function_name = get_sql_string(res->getString("func_name"));
        *message = get_sql_string(res->getString("message"));
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::remove_job(StorageConnection& conn, boost::uuids::uuid id)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "DELETE FROM `jobs` WHERE `id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::reset_job(StorageConnection& conn, boost::uuids::uuid const id)
        -> StorageErr {
    try {
        // Check for retry count on all tasks
        std::unique_ptr<sql::PreparedStatement> retry_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `id` FROM `tasks` WHERE `job_id` = ? AND `retry` >= `max_retry`"
                )
        );
        sql::bytes job_id_bytes = uuid_get_bytes(id);
        retry_statement->setBytes(1, &job_id_bytes);
        std::unique_ptr<sql::ResultSet> const res(retry_statement->executeQuery());
        if (res->rowsCount() > 0) {
            static_cast<MySqlConnection&>(conn)->commit();
            return StorageErr{StorageErrType::Success, "Some tasks have reached max retry count"};
        }
        // Increment the retry count for all tasks
        std::unique_ptr<sql::PreparedStatement> increment_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `retry` = `retry` + 1 WHERE `job_id` = ?"
                )
        );
        increment_statement->setBytes(1, &job_id_bytes);
        increment_statement->executeUpdate();
        // Reset states for all tasks. Head tasks should be ready and other tasks should be pending
        std::unique_ptr<sql::PreparedStatement> state_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `state` = IF(`id` NOT IN (SELECT `task_id` FROM "
                        "`task_inputs` WHERE `task_id` IN (SELECT `id` FROM `tasks` WHERE `job_id` "
                        "= ?) AND `output_task_id` IS NOT NULL), 'ready', 'pending') WHERE job_id "
                        "= ?"
                )
        );
        state_statement->setBytes(1, &job_id_bytes);
        state_statement->setBytes(2, &job_id_bytes);
        state_statement->executeUpdate();
        // Clear outputs for all tasks
        std::unique_ptr<sql::PreparedStatement> output_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `task_outputs` SET `value` = NULL, `data_id` = NULL WHERE "
                        "`task_id` IN (SELECT `id` FROM `tasks` WHERE `job_id` = ?)"
                )
        );
        output_statement->setBytes(1, &job_id_bytes);
        output_statement->executeUpdate();
        // Clear inputs for non-head tasks
        std::unique_ptr<sql::PreparedStatement> input_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `task_inputs` SET `value` = NULL, `data_id` = NULL WHERE `task_id` "
                        "IN (SELECT `id` FROM `tasks` WHERE `job_id` = ?) AND `output_task_id` IS "
                        "NOT NULL"
                )
        );
        input_statement->setBytes(1, &job_id_bytes);
        input_statement->executeUpdate();
        // Reset job state
        std::unique_ptr<sql::PreparedStatement> job_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `jobs` SET `state` = 'running' WHERE `id` = ?"
                )
        );
        job_statement->setBytes(1, &job_id_bytes);
        job_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_child(
        StorageConnection& conn,
        boost::uuids::uuid parent_id,
        Task const& child
) -> StorageErr {
    try {
        sql::bytes const job_id = uuid_get_bytes(child.get_id());
        add_task(static_cast<MySqlConnection&>(conn), job_id, child, std::nullopt);

        // Add dependencies
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `task_dependencies` (`parent`, `child`) VALUES (?, ?)"
                )
        );
        sql::bytes parent_id_bytes = uuid_get_bytes(parent_id);
        sql::bytes child_id_bytes = uuid_get_bytes(child.get_id());
        statement->setBytes(1, &parent_id_bytes);
        statement->setBytes(2, &child_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task(StorageConnection& conn, boost::uuids::uuid id, Task* task)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` WHERE `id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no task with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *task = fetch_full_task(static_cast<MySqlConnection&>(conn), res);
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_job_id(
        StorageConnection& conn,
        boost::uuids::uuid id,
        boost::uuids::uuid* job_id
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `job_id` FROM `tasks` WHERE `id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no task with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *job_id = read_id(res->getBinaryStream("job_id"));
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

constexpr int cLeaseExpireTime = 1000 * 10;  // 10 ms

auto MySqlMetadataStorage::get_ready_tasks(
        StorageConnection& conn,
        boost::uuids::uuid scheduler_id,
        std::vector<ScheduleTaskMetadata>* tasks
) -> StorageErr {
    try {
        // Remove timeout scheduler leases
        std::unique_ptr<sql::PreparedStatement> lease_timeout_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "DELETE FROM `scheduler_leases` WHERE TIMESTAMPDIFF(MICROSECOND, "
                        "`lease_time`, CURRENT_TIMESTAMP()) > ?"
                )
        );
        lease_timeout_statement->setInt(1, cLeaseExpireTime);
        lease_timeout_statement->executeUpdate();

        // Get all ready tasks from job that has not failed or cancelled
        std::unique_ptr<sql::Statement> task_statement(
                static_cast<MySqlConnection&>(conn)->createStatement()
        );
        std::unique_ptr<sql::ResultSet> const res{task_statement->executeQuery(
                "SELECT `id`, `func_name`, `job_id` FROM `tasks` WHERE `state` = 'ready' "
                "AND `job_id` NOT IN (SELECT `id` FROM `jobs` WHERE `state` != 'running') AND `id` "
                "NOT IN (SELECT `task_id` FROM `scheduler_leases`)"
        )};

        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->commit();
            return StorageErr{};
        }

        absl::flat_hash_map<boost::uuids::uuid, ScheduleTaskMetadata> new_tasks;
        absl::flat_hash_map<boost::uuids::uuid, std::vector<boost::uuids::uuid>> job_id_to_task_ids;
        while (res->next()) {
            boost::uuids::uuid const task_id = read_id(res->getBinaryStream("id"));
            boost::uuids::uuid const job_id = read_id(res->getBinaryStream("job_id"));
            std::string const function_name = get_sql_string(res->getString("func_name"));
            new_tasks.emplace(task_id, ScheduleTaskMetadata{task_id, function_name, job_id});
            if (job_id_to_task_ids.find(job_id) == job_id_to_task_ids.end()) {
                job_id_to_task_ids[job_id] = std::vector<boost::uuids::uuid>{task_id};
            } else {
                job_id_to_task_ids[job_id].emplace_back(task_id);
            }
        }

        // Get all job metadata
        std::unique_ptr<sql::Statement> job_statement{
                static_cast<MySqlConnection&>(conn)->createStatement()
        };
        std::unique_ptr<sql::ResultSet> const job_res{job_statement->executeQuery(
                "SELECT `id` , `client_id` , `creation_time` FROM `jobs` WHERE `id` IN (SELECT "
                "`id` FROM `jobs` WHERE `state` = 'running')"
        )};

        // Get job metadata
        while (job_res->next()) {
            boost::uuids::uuid const job_id = read_id(job_res->getBinaryStream("id"));
            boost::uuids::uuid const client_id = read_id(job_res->getBinaryStream("client_id"));
            std::optional<std::chrono::system_clock::time_point> const optional_creation_time
                    = parse_timestamp(get_sql_string(job_res->getString("creation_time")));
            if (false == optional_creation_time.has_value()) {
                static_cast<MySqlConnection&>(conn)->rollback();
                return StorageErr{
                        StorageErrType::OtherErr,
                        fmt::format(
                                "Cannot parse timestamp {}",
                                get_sql_string(job_res->getString("creation_time"))
                        )
                };
            }
            // Job id will not be in job_id_to_task_ids if the job's tasks are leased
            if (job_id_to_task_ids.find(job_id) == job_id_to_task_ids.end()) {
                continue;
            }
            for (boost::uuids::uuid const& task_id : job_id_to_task_ids[job_id]) {
                new_tasks[task_id].set_client_id(client_id);
                new_tasks[task_id].set_job_creation_time(optional_creation_time.value());
            }
        }

        // Get all data localities
        std::unique_ptr<sql::Statement> locality_statement{
                static_cast<MySqlConnection&>(conn)->createStatement()
        };
        std::unique_ptr<sql::ResultSet> const locality_res{locality_statement->executeQuery(
                "SELECT `task_inputs`.`task_id`, `data`.`hard_locality`, "
                "`data_locality`.`address` FROM `task_inputs` JOIN `data` ON "
                "`task_inputs`.`data_id` = `data`.`id` JOIN `data_locality` ON `data`.`id` "
                "= `data_locality`.`id` WHERE `task_inputs`.`task_id` IN (SELECT `id` "
                "FROM `tasks` WHERE `state` = 'ready' AND `job_id` NOT IN (SELECT `id` FROM `jobs` "
                "WHERE `state` != 'running')) AND `task_inputs`.`task_id` NOT IN (SELECT `task_id` "
                "FROM `scheduler_leases`)"
        )};

        while (locality_res->next()) {
            boost::uuids::uuid const task_id = read_id(locality_res->getBinaryStream("task_id"));
            bool const hard_locality = locality_res->getBoolean("hard_locality");
            std::string const address = get_sql_string(locality_res->getString("address"));
            if (hard_locality) {
                new_tasks[task_id].add_hard_locality(address);
            } else {
                new_tasks[task_id].add_soft_locality(address);
            }
        }

        // Add scheduler lease
        std::unique_ptr<sql::PreparedStatement> lease_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `scheduler_leases` (`scheduler_id`, `task_id`) VALUES (?, ?)"
                )
        );
        sql::bytes scheduler_id_bytes = uuid_get_bytes(scheduler_id);
        for (auto const& [task_id, task] : new_tasks) {
            sql::bytes task_id_bytes = uuid_get_bytes(task_id);
            lease_statement->setBytes(1, &scheduler_id_bytes);
            lease_statement->setBytes(2, &task_id_bytes);
            lease_statement->addBatch();
        }
        lease_statement->executeBatch();

        // Add all tasks to the output
        absl::flat_hash_set<boost::uuids::uuid> task_ids;
        for (ScheduleTaskMetadata const& task : *tasks) {
            task_ids.insert(task.get_id());
        }
        for (auto const& [task_id, task] : new_tasks) {
            if (task_ids.find(task_id) == task_ids.end()) {
                tasks->emplace_back(task);
            }
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::set_task_state(
        StorageConnection& conn,
        boost::uuids::uuid id,
        TaskState state
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `state` = ? WHERE `id` = ?"
                )
        );
        statement->setString(1, task_state_to_string(state));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(2, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErKeyNotFound) {
            return StorageErr{StorageErrType::KeyNotFoundErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_state(
        StorageConnection& conn,
        boost::uuids::uuid const id,
        TaskState* state
) -> StorageErr {
    try {
        // Get the state of the task
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `state` FROM `tasks` WHERE `id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->commit();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("No task with id {} ", boost::uuids::to_string(id))
            };
        }
        res->next();
        std::string const state_str = get_sql_string(res->getString("state"));
        *state = string_to_task_state(state_str);
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::set_task_running(StorageConnection& conn, boost::uuids::uuid id)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `state` = 'running' WHERE `id` = ? AND `state` = "
                        "'ready'"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        int32_t const update_count = statement->executeUpdate();
        if (update_count == 0) {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{StorageErrType::KeyNotFoundErr, "Task not ready"};
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::add_task_instance(StorageConnection& conn, TaskInstance const& instance)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> const statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `task_instances` (`id`, `task_id`, `start_time`) VALUES(?, ?, "
                        "CURRENT_TIMESTAMP())"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(instance.id);
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto
MySqlMetadataStorage::create_task_instance(StorageConnection& conn, TaskInstance const& instance)
        -> StorageErr {
    try {
        // Check the state of the task
        std::unique_ptr<sql::PreparedStatement> ready_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `state` FROM `tasks` WHERE `id` = ? AND `state` = 'ready'"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(instance.task_id);
        ready_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const ready_res(ready_statement->executeQuery());
        bool const task_ready = ready_res->rowsCount() > 0;
        // Check all task instances have timed out
        std::unique_ptr<sql::PreparedStatement> not_timeout_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `t1`.`id` FROM `task_instances` as `t1` JOIN `tasks` ON "
                        "`t1`.`task_id` = `tasks`.`id` WHERE `t1`.`task_id` = ? AND "
                        "(`tasks`.`timeout` < 0.0001 OR TIMESTAMPDIFF(MICROSECOND, "
                        "`t1`.`start_time`, CURRENT_TIMESTAMP()) < `tasks`.`timeout` * 1000)"
                )
        );
        not_timeout_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const not_timeout_res(
                not_timeout_statement->executeQuery()
        );
        bool const all_timeout = not_timeout_res->rowsCount() == 0;
        if (!task_ready && !all_timeout) {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{StorageErrType::OtherErr, "Task not ready or timed out"};
        }
        // Check the job state
        std::unique_ptr<sql::PreparedStatement> job_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `state` FROM `jobs` WHERE `id` = (SELECT `job_id` FROM "
                        "`tasks` WHERE `id` = ?)"
                )
        );
        job_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const job_res(job_statement->executeQuery());
        if (job_res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{StorageErrType::KeyNotFoundErr, "Job not found"};
        }
        job_res->next();
        std::string const job_state = get_sql_string(job_res->getString("state"));
        if (job_state != "running") {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{StorageErrType::OtherErr, "Job state wrong"};
        }
        // Set the task state to running
        std::unique_ptr<sql::PreparedStatement> const running_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `state` = 'running' WHERE `id` = ?"
                )
        );
        running_statement->setBytes(1, &id_bytes);
        running_statement->executeUpdate();
        // Insert task instance
        std::unique_ptr<sql::PreparedStatement> const instance_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `task_instances` (`id`, `task_id`, `start_time`) VALUES(?, ?, "
                        "CURRENT_TIMESTAMP())"
                )
        );
        sql::bytes instance_id_bytes = uuid_get_bytes(instance.id);
        instance_statement->setBytes(1, &instance_id_bytes);
        instance_statement->setBytes(2, &id_bytes);
        instance_statement->executeUpdate();
        // Remove task from scheduler leases
        std::unique_ptr<sql::PreparedStatement> const lease_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "DELETE FROM `scheduler_leases` WHERE `task_id` = ?"
                )
        );
        lease_statement->setBytes(1, &id_bytes);
        lease_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }

    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::task_finish(
        StorageConnection& conn,
        TaskInstance const& instance,
        std::vector<TaskOutput> const& outputs
) -> StorageErr {
    try {
        // Try to submit task instance
        std::unique_ptr<sql::PreparedStatement> const statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `instance_id` = ?, `state` = 'success' WHERE `id` = ? "
                        "AND `instance_id` is NULL AND `state` = 'running'"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(instance.id);
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        int32_t const update_count = statement->executeUpdate();
        if (update_count == 0) {
            static_cast<MySqlConnection&>(conn)->commit();
            return StorageErr{};
        }

        // Update task outputs
        std::unique_ptr<sql::PreparedStatement> output_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `task_outputs` SET `value` = ?, `data_id` = ? WHERE `task_id` = ? "
                        "AND `position` = ?"
                )
        );
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
        std::unique_ptr<sql::PreparedStatement> input_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `task_inputs` SET `value` = ?, `data_id` = ? WHERE "
                        "`output_task_id` = ? AND `output_task_position` = ?"
                )
        );
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
        std::unique_ptr<sql::PreparedStatement> ready_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `tasks` SET `state` = 'ready' WHERE `id` IN (SELECT `task_id` FROM "
                        "`task_inputs` WHERE `output_task_id` = ?) AND `state` = 'pending' AND NOT "
                        "EXISTS (SELECT `task_id` FROM `task_inputs` WHERE `task_id` IN (SELECT "
                        "`task_id` FROM `task_inputs` WHERE `output_task_id` = ?) AND `value` IS "
                        "NULL AND `data_id` IS NULL)"
                )
        );
        ready_statement->setBytes(1, &task_id_bytes);
        ready_statement->setBytes(2, &task_id_bytes);
        ready_statement->executeUpdate();
        // If all tasks in the job finishes, set the job state to success
        std::unique_ptr<sql::PreparedStatement> job_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `jobs` SET `state` = 'success' WHERE `id` = (SELECT `job_id` FROM "
                        "`tasks` WHERE `id` = ?) AND NOT EXISTS (SELECT `job_id` FROM `tasks` "
                        "WHERE `job_id` = (SELECT `job_id` FROM `tasks` WHERE `id` = ?) AND "
                        "`state` != 'success') AND `state` = 'running'"
                )
        );
        job_statement->setBytes(1, &task_id_bytes);
        job_statement->setBytes(2, &task_id_bytes);
        job_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        if (e.getErrorCode() == ErDeadLock) {
            return StorageErr{StorageErrType::DeadLockErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::task_fail(
        StorageConnection& conn,
        TaskInstance const& instance,
        std::string const& /*error*/
) -> StorageErr {
    try {
        // Remove task instance
        std::unique_ptr<sql::PreparedStatement> const statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "DELETE FROM `task_instances` WHERE `id` = ?"
                )
        );
        sql::bytes instance_id_bytes = uuid_get_bytes(instance.id);
        statement->setBytes(1, &instance_id_bytes);
        statement->executeUpdate();

        // Get number of remaining instances
        std::unique_ptr<sql::PreparedStatement> const count_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT COUNT(*) FROM `task_instances` WHERE `task_id` = ?"
                )
        );
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        count_statement->setBytes(1, &task_id_bytes);
        std::unique_ptr<sql::ResultSet> const count_res{count_statement->executeQuery()};
        count_res->next();
        int32_t const count = count_res->getInt(1);
        if (count == 0) {
            // Set the task fail if the last task instance fails
            std::unique_ptr<sql::PreparedStatement> const task_statement(
                    static_cast<MySqlConnection&>(conn)->prepareStatement(
                            "UPDATE `tasks` SET `state` = 'fail' WHERE `id` = ? AND `state` = "
                            "'running'"
                    )
            );
            task_statement->setBytes(1, &task_id_bytes);
            int32_t const task_count = task_statement->executeUpdate();
            if (task_count == 0) {
                static_cast<MySqlConnection&>(conn)->commit();
                return StorageErr{};
            }
            // Set the job fails
            std::unique_ptr<sql::PreparedStatement> const job_statement(
                    static_cast<MySqlConnection&>(conn)->prepareStatement(
                            "UPDATE `jobs` SET `state` = 'fail' WHERE `id` = (SELECT `job_id` FROM "
                            "`tasks` WHERE `id` = ?)"
                    )
            );
            job_statement->setBytes(1, &task_id_bytes);
            job_statement->executeUpdate();
        }
    } catch (sql::SQLException& e) {
        spdlog::error("Task fail error: {}", e.what());
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_task_timeout(
        StorageConnection& conn,
        std::vector<ScheduleTaskMetadata>* tasks
) -> StorageErr {
    try {
        std::unique_ptr<sql::Statement> task_statement(
                static_cast<MySqlConnection&>(conn)->createStatement()
        );

        std::unique_ptr<sql::ResultSet> const task_res{task_statement->executeQuery(
                "SELECT `id`, `func_name`, `job_id` FROM `tasks` WHERE `id` IN (SELECT "
                "`tasks`.`id` FROM `task_instances` JOIN `tasks` ON `task_instances`.`task_id` = "
                "`tasks`.`id` WHERE `tasks`.`timeout` > 0.0001 AND `tasks`.`state` = 'running' AND "
                "TIMESTAMPDIFF(MICROSECOND, `task_instances`.`start_time`, CURRENT_TIMESTAMP()) > "
                "`tasks`.`timeout` * 1000) AND `id` NOT IN (SELECT `tasks`.`id` FROM "
                "`task_instances` JOIN `tasks` ON `task_instances`.`task_id` = `tasks`.`id` WHERE "
                "`tasks`.`timeout` > 0.0001 AND `tasks`.`state` = 'running' AND "
                "TIMESTAMPDIFF(MICROSECOND, `task_instances`.`start_time`, CURRENT_TIMESTAMP()) < "
                "`tasks`.` timeout` * 1000)"
        )};

        absl::flat_hash_map<boost::uuids::uuid, ScheduleTaskMetadata> new_tasks;
        absl::flat_hash_map<boost::uuids::uuid, std::vector<boost::uuids::uuid>> job_id_to_task_ids;
        while (task_res->next()) {
            boost::uuids::uuid const task_id = read_id(task_res->getBinaryStream("id"));
            boost::uuids::uuid const job_id = read_id(task_res->getBinaryStream("job_id"));
            std::string const function_name = get_sql_string(task_res->getString("func_name"));
            new_tasks.emplace(task_id, ScheduleTaskMetadata{task_id, function_name, job_id});
            if (job_id_to_task_ids.find(job_id) == job_id_to_task_ids.end()) {
                job_id_to_task_ids[job_id] = std::vector<boost::uuids::uuid>{task_id};
            } else {
                job_id_to_task_ids[job_id].emplace_back(task_id);
            }
        }

        // Get all job metadata
        std::unique_ptr<sql::Statement> job_statement{
                static_cast<MySqlConnection&>(conn)->createStatement()
        };
        std::unique_ptr<sql::ResultSet> const job_res{job_statement->executeQuery(
                "SELECT `jobs`.`id`, `jobs`.`client_id`, `jobs`.`creation_time` FROM `jobs` JOIN "
                "`tasks` ON `jobs`.`id` = `tasks`.`job_id` WHERE `tasks`.`id` IN (SELECT "
                "`tasks`.`id` FROM `task_instances` JOIN `tasks` ON `task_instances`.`task_id` = "
                "`tasks`.`id` WHERE `tasks`.`timeout` > 0.0001 AND `tasks`.`state` = 'running' AND "
                "TIMESTAMPDIFF(MICROSECOND, `task_instances`.`start_time`, CURRENT_TIMESTAMP()) > "
                "`tasks`.`timeout` * 1000) AND `tasks`.`id` NOT IN (SELECT `tasks`.`id` FROM "
                "`task_instances` JOIN `tasks` ON `task_instances`.`task_id` = `tasks`.`id` WHERE "
                "`tasks`.`timeout` > 0.0001 AND `tasks`.`state` = 'running' AND "
                "TIMESTAMPDIFF(MICROSECOND, `task_instances`.`start_time`, CURRENT_TIMESTAMP()) < "
                "`tasks`.` timeout` * 1000)"
        )};

        while (job_res->next()) {
            boost::uuids::uuid const job_id = read_id(job_res->getBinaryStream("id"));
            boost::uuids::uuid const client_id = read_id(job_res->getBinaryStream("client_id"));
            std::optional<std::chrono::system_clock::time_point> const optional_creation_time
                    = parse_timestamp(get_sql_string(job_res->getString("creation_time")));
            if (false == optional_creation_time.has_value()) {
                static_cast<MySqlConnection&>(conn)->rollback();
                return StorageErr{
                        StorageErrType::OtherErr,
                        fmt::format(
                                "Cannot parse timestamp {}",
                                get_sql_string(job_res->getString("creation_time"))
                        )
                };
            }
            for (boost::uuids::uuid const& task_id : job_id_to_task_ids[job_id]) {
                new_tasks[task_id].set_client_id(client_id);
                new_tasks[task_id].set_job_creation_time(optional_creation_time.value());
            }
        }

        // Get all data localities
        std::unique_ptr<sql::Statement> locality_statement{
                static_cast<MySqlConnection&>(conn)->createStatement()
        };
        std::unique_ptr<sql::ResultSet> const locality_res{locality_statement->executeQuery(
                "SELECT `task_inputs`.`task_id`, `data`.`hard_locality`, `data_locality`.`address` "
                "FROM `task_inputs` JOIN `data` ON `task_inputs`.`data_id` = `data`.`id` JOIN "
                "`data_locality` ON `data`.`id` = `data_locality`.`id` WHERE "
                "`task_inputs`.`task_id` IN (SELECT `tasks`.`id` FROM `task_instances` JOIN "
                "`tasks` ON `task_instances`.`task_id` = `tasks`.`id` WHERE `tasks`.`timeout` > "
                "0.0001 AND `tasks`.`state` = 'running' AND TIMESTAMPDIFF(MICROSECOND, "
                "`task_instances`.`start_time`, CURRENT_TIMESTAMP()) > `tasks`.`timeout` * 1000) "
                "AND `task_inputs`.`task_id` NOT IN (SELECT `tasks`.`id` FROM `task_instances` "
                "JOIN `tasks` ON `task_instances`.`task_id` = `tasks`.`id` WHERE `tasks`.`timeout` "
                "> 0.0001 AND `tasks`.`state` = 'running' AND TIMESTAMPDIFF(MICROSECOND, "
                "`task_instances`.`start_time`, CURRENT_TIMESTAMP()) < `tasks`.` timeout` * 1000)"
        )};

        while (locality_res->next()) {
            boost::uuids::uuid const task_id = read_id(locality_res->getBinaryStream("task_id"));
            bool const hard_locality = locality_res->getBoolean("hard_locality");
            std::string const address = get_sql_string(locality_res->getString("address"));
            if (hard_locality) {
                new_tasks[task_id].add_hard_locality(address);
            } else {
                new_tasks[task_id].add_soft_locality(address);
            }
        }

        // Add all tasks to the output
        absl::flat_hash_set<boost::uuids::uuid> task_ids;
        for (ScheduleTaskMetadata const& task : *tasks) {
            task_ids.insert(task.get_id());
        }
        for (auto const& [task_id, task] : new_tasks) {
            if (task_ids.find(task_id) == task_ids.end()) {
                tasks->emplace_back(task);
            }
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_child_tasks(
        StorageConnection& conn,
        boost::uuids::uuid id,
        std::vector<Task>* children
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` JOIN "
                        "`task_dependencies` as `t2` WHERE `tasks`.`id` = `t2`.`child` AND "
                        "`t2`.`parent` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        while (res->next()) {
            children->emplace_back(fetch_full_task(static_cast<MySqlConnection&>(conn), res));
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_parent_tasks(
        StorageConnection& conn,
        boost::uuids::uuid id,
        std::vector<Task>* tasks
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `id`, `func_name`, `state`, `timeout` FROM `tasks` JOIN "
                        "`task_dependencies` as `t2` WHERE `tasks`.`id` = `t2`.`parent` AND "
                        "`t2`.`child` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const res(statement->executeQuery());
        while (res->next()) {
            tasks->emplace_back(fetch_full_task(static_cast<MySqlConnection&>(conn), res));
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::update_heartbeat(StorageConnection& conn, boost::uuids::uuid id)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `drivers` SET `heartbeat` = CURRENT_TIMESTAMP() WHERE `id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

namespace {
constexpr int cMillisecondToMicrosecond = 1000;
}  // namespace

auto MySqlMetadataStorage::heartbeat_timeout(
        StorageConnection& conn,
        double timeout,
        std::vector<boost::uuids::uuid>* ids
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `id` FROM `drivers` WHERE TIMESTAMPDIFF(MICROSECOND, `heartbeat`, "
                        "CURRENT_TIMESTAMP()) > ?"
                )
        );
        statement->setDouble(1, timeout * cMillisecondToMicrosecond);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        while (res->next()) {
            ids->emplace_back(read_id(res->getBinaryStream("id")));
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlMetadataStorage::get_scheduler_addr(
        StorageConnection& conn,
        boost::uuids::uuid id,
        std::string* addr,
        int* port
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `address`, `port` FROM `schedulers` WHERE `id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res{statement->executeQuery()};
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->rollback();
            return StorageErr{
                    StorageErrType::KeyNotFoundErr,
                    fmt::format("no scheduler with id {}", boost::uuids::to_string(id))
            };
        }
        res->next();
        *addr = get_sql_string(res->getString(1));
        *port = res->getInt(2);
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::initialize(StorageConnection& conn) -> StorageErr {
    try {
        // Need to initialize metadata storage first so that foreign constraint is not voilated
        for (std::string const& create_table_str : mysql::cCreateStorage) {
            std::unique_ptr<sql::Statement> statement(
                    static_cast<MySqlConnection&>(conn)->createStatement()
            );
            statement->executeUpdate(create_table_str);
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }

    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_driver_data(
        StorageConnection& conn,
        boost::uuids::uuid const driver_id,
        Data const& data
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data` (`id`, `value`, `hard_locality`) VALUES(?, ?, ?)"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(1, &id_bytes);
        statement->setString(2, data.get_value());
        statement->setBoolean(3, data.is_hard_locality());
        statement->executeUpdate();

        for (std::string const& addr : data.get_locality()) {
            std::unique_ptr<sql::PreparedStatement> locality_statement(
                    static_cast<MySqlConnection&>(conn)->prepareStatement(
                            "INSERT INTO `data_locality` (`id`, "
                            "`address`) VALUES (?, ?)"
                    )
            );
            locality_statement->setBytes(1, &id_bytes);
            locality_statement->setString(2, addr);
            locality_statement->executeUpdate();
        }
        std::unique_ptr<sql::PreparedStatement> driver_ref_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data_ref_driver` (`id`, `driver_id`) VALUES(?, ?)"
                )
        );
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        driver_ref_statement->setBytes(1, &id_bytes);
        driver_ref_statement->setBytes(2, &driver_id_bytes);
        driver_ref_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_data(
        StorageConnection& conn,
        boost::uuids::uuid const task_id,
        Data const& data
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data` (`id`, `value`, `hard_locality`) VALUES(?, ?, ?)"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(1, &id_bytes);
        statement->setString(2, data.get_value());
        statement->setBoolean(3, data.is_hard_locality());
        statement->executeUpdate();

        for (std::string const& addr : data.get_locality()) {
            std::unique_ptr<sql::PreparedStatement> locality_statement(
                    static_cast<MySqlConnection&>(conn)->prepareStatement(
                            "INSERT INTO `data_locality` (`id`, `address`) VALUES (?, ?)"
                    )
            );
            locality_statement->setBytes(1, &id_bytes);
            locality_statement->setString(2, addr);
            locality_statement->executeUpdate();
        }
        std::unique_ptr<sql::PreparedStatement> task_ref_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data_ref_task` (`id`, `task_id`) VALUES(?, ?)"
                )
        );
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        task_ref_statement->setBytes(1, &id_bytes);
        task_ref_statement->setBytes(2, &task_id_bytes);
        task_ref_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_data_with_locality(
        StorageConnection& conn,
        boost::uuids::uuid const id,
        Data* data
) -> StorageErr {
    std::unique_ptr<sql::PreparedStatement> statement(
            static_cast<MySqlConnection&>(conn)->prepareStatement(
                    "SELECT `id`, `value`, `hard_locality` FROM `data` WHERE `id` = ?"
            )
    );
    sql::bytes id_bytes = uuid_get_bytes(id);
    statement->setBytes(1, &id_bytes);
    std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
    if (res->rowsCount() == 0) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{
                StorageErrType::KeyNotFoundErr,
                fmt::format("no data with id {}", boost::uuids::to_string(id))
        };
    }
    res->next();
    *data = Data{id, get_sql_string(res->getString(2))};
    data->set_hard_locality(res->getBoolean(3));

    std::unique_ptr<sql::PreparedStatement> locality_statement(
            static_cast<MySqlConnection&>(conn)->prepareStatement(
                    "SELECT `address` FROM `data_locality` WHERE `id` = ?"
            )
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
    return StorageErr{};
}

auto MySqlDataStorage::get_data(StorageConnection& conn, boost::uuids::uuid const id, Data* data)
        -> StorageErr {
    try {
        StorageErr const err = get_data_with_locality(conn, id, data);
        if (false == err.success()) {
            return err;
        }
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_driver_data(
        StorageConnection& conn,
        boost::uuids::uuid const driver_id,
        boost::uuids::uuid const data_id,
        Data* data
) -> StorageErr {
    try {
        StorageErr const err = get_data_with_locality(conn, data_id, data);
        if (false == err.success()) {
            return err;
        }
        // Add data reference from driver
        std::unique_ptr<sql::PreparedStatement> statement{
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data_ref_driver` (`id`, `driver_id`) VALUES (?, ?)"
                )
        };
        sql::bytes id_bytes = uuid_get_bytes(data_id);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_task_data(
        StorageConnection& conn,
        boost::uuids::uuid const task_id,
        boost::uuids::uuid const data_id,
        Data* data
) -> StorageErr {
    try {
        StorageErr const err = get_data_with_locality(conn, data_id, data);
        if (false == err.success()) {
            return err;
        }
        // Add data reference from task
        std::unique_ptr<sql::PreparedStatement> statement{
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data_ref_task` (`id`, `task_id`) VALUES (?, ?)"
                )
        };
        sql::bytes id_bytes = uuid_get_bytes(data_id);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::set_data_locality(StorageConnection& conn, Data const& data) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> const delete_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "DELETE FROM `data_locality` WHERE `id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        delete_statement->setBytes(1, &id_bytes);
        delete_statement->executeUpdate();
        std::unique_ptr<sql::PreparedStatement> const insert_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data_locality` (`id`, `address`) VALUES(?, ?)"
                )
        );
        for (std::string const& addr : data.get_locality()) {
            insert_statement->setBytes(1, &id_bytes);
            insert_statement->setString(2, addr);
            insert_statement->executeUpdate();
        }
        std::unique_ptr<sql::PreparedStatement> const hard_locality_statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "UPDATE `data` SET `hard_locality` = ? WHERE `id` = ?"
                )
        );
        hard_locality_statement->setBoolean(1, data.is_hard_locality());
        hard_locality_statement->setBytes(2, &id_bytes);
        hard_locality_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_data(StorageConnection& conn, boost::uuids::uuid id) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "DELETE FROM `data` WHERE `id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_reference(
        StorageConnection& conn,
        boost::uuids::uuid id,
        boost::uuids::uuid task_id
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data_ref_task` (`id`, `task_id`) VALUES(?, ?)"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_task_reference(
        StorageConnection& conn,
        boost::uuids::uuid id,
        boost::uuids::uuid task_id
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "DELETE FROM `data_ref_task` WHERE `id` = ? AND `task_id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_driver_reference(
        StorageConnection& conn,
        boost::uuids::uuid id,
        boost::uuids::uuid driver_id
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `data_ref_driver` (`id`, `driver_id`) VALUES(?, ?)"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_driver_reference(
        StorageConnection& conn,
        boost::uuids::uuid id,
        boost::uuids::uuid driver_id
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "DELETE FROM `data_ref_driver` WHERE `id` = ? AND `driver_id` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::remove_dangling_data(StorageConnection& conn) -> StorageErr {
    try {
        std::unique_ptr<sql::Statement> statement{
                static_cast<MySqlConnection&>(conn)->createStatement()
        };
        statement->execute(
                "DELETE FROM `data` WHERE `id` NOT IN (SELECT driver_ref.`id` FROM "
                "`data_ref_driver` driver_ref) AND `id` NOT IN (SELECT task_ref.`id` "
                "FROM `data_ref_task` task_ref)"
        );
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_client_kv_data(StorageConnection& conn, KeyValueData const& data)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `client_kv_data` (`kv_key`, `value`, `client_id`) VALUES(?, "
                        "?, ?)"
                )
        );
        statement->setString(1, data.get_key());
        statement->setString(2, data.get_value());
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(3, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::add_task_kv_data(StorageConnection& conn, KeyValueData const& data)
        -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "INSERT INTO `task_kv_data` (`kv_key`, `value`, `task_id`) VALUES(?, ?, ?)"
                )
        );
        statement->setString(1, data.get_key());
        statement->setString(2, data.get_value());
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(3, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        static_cast<MySqlConnection&>(conn)->rollback();
        if (e.getErrorCode() == ErDupKey || e.getErrorCode() == ErDupEntry) {
            return StorageErr{StorageErrType::DuplicateKeyErr, e.what()};
        }
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_client_kv_data(
        StorageConnection& conn,
        boost::uuids::uuid const& client_id,
        std::string const& key,
        std::string* value
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `value` FROM `client_kv_data` WHERE `client_id` = ? AND `kv_key` = "
                        "?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(client_id);
        statement->setBytes(1, &id_bytes);
        statement->setString(2, key);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->rollback();
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
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

auto MySqlDataStorage::get_task_kv_data(
        StorageConnection& conn,
        boost::uuids::uuid const& task_id,
        std::string const& key,
        std::string* value
) -> StorageErr {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                static_cast<MySqlConnection&>(conn)->prepareStatement(
                        "SELECT `value` FROM `task_kv_data` WHERE `task_id` = ? AND `kv_key` = ?"
                )
        );
        sql::bytes id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(1, &id_bytes);
        statement->setString(2, key);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            static_cast<MySqlConnection&>(conn)->rollback();
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
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)
}  // namespace spider::core
