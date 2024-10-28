#pragma clang diagnostic push
#pragma ide diagnostic ignored "modernize-use-trailing-return-type"
#include "MysqlStorage.hpp"

#include <boost/uuid/uuid_io.hpp>

// mariadb-connector-cpp does not define SQL errcode. Just include some useful ones.
#define ER_CANT_CREATE_TABLE 1005
#define ER_CANT_CREATE_DB 1006
#define ER_DUP_KEY 1022
#define ER_KEY_NOT_FOUND 1032
#define ER_DUP_ENTRY 1062
#define ER_WRONG_DB_NAME 1102
#define ER_WRONG_TABLE_NAME 1103
#define ER_UNKNOWN_TABLE 1109

namespace spider::core {

char const* cCreateDriverTable = R"(CREATE TABLE IF NOT EXISTS drivers (
    id BINARY(16) NOT NULL,
    address INT UNSIGNED NOT NULL,
    heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
))";

char const* cCreateSchedulerTable = R"(CREATE TABLE IF NOT EXISTS schedulers (
    id BINARY(16) NOT NULL,
    port INT UNSIGNED NOT NULL,
    state ENUM('normal', 'recovery', 'gc') NOT NULL,
    CONSTRAINT scheduler_driver_id FOREIGN KEY (id) REFERENCES drivers (id) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (id)
))";

char const* cCreateTaskTable = R"(CREATE TABLE IF NOT EXISTS tasks (
    id BINARY(16) NOT NULL,
    job_id BINARY(16) NOT NULL, -- for performance only
    func_name VARCHAR(64) NOT NULL,
    state ENUM('pending', 'ready', 'running', 'success', 'cancel', 'fail') NOT NULL,
    creator BINARY(64), -- used when task is created by task
    timeout FLOAT,
    max_retry INT UNSIGNED DEFAULT 0,
    instance_id BINARY(16),
    KEY job_id USING BTREE,
    PRIMARY KEY (id)
))";

char const* cCreateTaskInputTable = R"(CREATE TABLE IF NOT EXISTS task_inputs (
    task_id BINARY(16) NOT NULL,
    position INT UNSIGNED NOT NULL,
    type VARCHAR(64) NOT NULL,
    output_task_id BINARY(16),
    output_task_position INT UNSIGNED,
    value VARCHAR(64), -- Use VARCHAR for all types of values
    data_id BINARY(16),
    CONSTRAINT input_task_id FOREIGN KEY (task_id) REFERENCES tasks (id) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT input_task_output_match FOREIGN KEY (output_task_id, output_task_position) REFERENCES task_outputs (task_id, position) ON UPDATE NO ACTION ON DELETE SET NULL,
    CONSTRAINT input_data_id FOREIGN KEY (data_id) REFERENCES data (id) ON UPDATE NO ACTION ON DELETE NO ACTION,
    PRIMARY KEY (task_id, position)
))";

char const* cCreateTaskOutputTable = R"(CREATE TABLE IF NOT EXISTS task_outputs(
    task_id BINARY(16) NOT NULL,
    position INT UNSIGNED NOT NULL,
    type VARCHAR(64) NOT NULL,
    value VARCHAR(64),
    data_id BINARY(16),
    CONSTRAINT output_task_id FOREIGN KEY (task_id) REFERENCES tasks (id) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT output_data_id FOREIGN KEY (data_id) REFERENCES data (id) ON UPDATE NO ACTION ON DELETE NO ACTION,
    PRIMARY KEY (task_id, position)
))";

char const* cCreateTaskDependencyTable = R"(CREATE TABLE IF NOT EXISTS task_dependencies (
    parent BINARY(16) NOT NULL,
    child BINARY(16) NOT NULL,
    KEY parent USING BTREE,
    KEY child USING BTREE,
    CONSTRAINT task_dep_parent FOREIGN KEY (parent) REFERENCES tasks (id) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT task_dep_child FOREIGN KEY (child) REFERENCES tasks (id) ON UPDATE NO ACTION ON DELETE CASCADE
))";

char const* cCreateTaskInstanceTable = R"(CREATE TABLE IF NOT EXISTS task_instances (
    id BINARY(16) NOT NULL,
    task_id BINARY(16) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    CONSTRAINT instance_task_id FOREIGN KEY (task_id) REFERENCES task (id) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (id)
))";

char const* cCreateFutureTable = R"(CREATE TABLE IF NOT EXISTS task_instances (
    id BINARY(16) NOT NULL,
    task_id BINARY(16) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    CONSTRAINT instance_task_id FOREIGN KEY (task_id) REFERENCES task (id) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (id)
))";

char const* cCreateMetadataStorage[] = {
        cCreateDriverTable,
        cCreateSchedulerTable,
        cCreateTaskTable,
        cCreateTaskInputTable,
        cCreateTaskOutputTable,
        cCreateTaskDependencyTable,
        cCreateTaskInstanceTable,
        cCreateFutureTable,
};

char const* cCreateDataTable = R"(CREATE TABLE IF NOT EXISTS data (
    id BINARY(16) NOT NULL,
    key VARCHAR(64),
    value VARCHAR(256) NOT NULL,
    hard_locality BOOL DEFAULT FALSE,
    gc BOOL DEFAULT FALSE,
    persisted BOOL DEFAULT FALSE,
    KEY key USING BTREE,
    PRIMARY KEY (id)
))";

char const* cCreateDataLocalityTable = R"(CREATE TABLE IF NOT EXISTS data_locality (
    id BINARY(16) NOT NULL,
    address INT UNSIGNED NOT NULL,
    KEY id USING BTREE,
    CONSTRAINT locality_data_id FOREIGN KEY (id) REFERENCES data (id) ON UPDATE NO ACTION ON DELETE CASCADE
))";

char const* cCreateDataRefDriverTable = R"(CREATE TABLE IF NOT EXISTS data_ref_driver (
    id BINARY(16) NOT NULL,
    driver_id BINARY(16) NOT NULL,
    KEY id USING BTREE,
    CONSTRAINT data_ref_id FOREIGN KEY (id) REFERENCES data (id) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT data_ref_driver_id FOREIGN KEY (driver_id) REFERENCES drivers (id) ON UPDATE NO ACTION ON DELETE CASCADE
))";

char const* cCreateDataRefTaskTable = R"(CREATE TABLE IF NOT EXISTS data_ref_task (
    id BINARY(16) NOT NULL,
    task_id BINARY(16) NOT NULL,
    KEY id USING BTREE,
    CONSTRAINT data_ref_id FOREIGN KEY (id) REFERENCES data (id) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT data_ref_task_id FOREIGN KEY (task_id) REFERENCES tasks (id) ON UPDATE NO ACTION ON DELETE CASCADE
))";

char const* cCreateDataStorage[] = {
        cCreateDataTable,
        cCreateDataLocalityTable,
        cCreateDataRefDriverTable,
        cCreateDataRefTaskTable,
};

static sql::bytes uuid_get_bytes(boost::uuids::uuid id) {
    return {(char*)id.data(), id.size()};
}

StorageErr MySqlMetadataStorage::connect(std::string const& url) {
    if (nullptr == m_conn) {
        try {
            sql::Driver* driver = sql::mariadb::get_driver_instance();
            sql::Properties properties;
            m_conn = driver->connect(sql::SQLString(url), properties);
            m_conn->setAutoCommit(false);
        } catch (sql::SQLException& e) {
            return StorageErr(StorageErrType::kConnectionErr, e.what());
        }
    }
}

void MySqlMetadataStorage::close() {
    if (nullptr != m_conn) {
        m_conn->close();
        m_conn = nullptr;
    }
}

StorageErr MySqlMetadataStorage::initialize() {
    try {
        for (char const* create_table_str : cCreateMetadataStorage) {
            std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
            statement->executeUpdate(create_table_str);
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_CANT_CREATE_TABLE) {
            return StorageErr(StorageErrType::kCreateStorageErr, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }

    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::add_driver(boost::uuids::uuid id, std::string const& addr) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("INSERT INTO drivers (id, address) VALUES (?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->setString(2, addr);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr
MySqlMetadataStorage::add_driver(boost::uuids::uuid id, std::string const& addr, int port) {
    try {
        std::unique_ptr<sql::PreparedStatement> driver_statement(
                m_conn->prepareStatement("INSERT INTO drivers (id, address) VALUES (?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        driver_statement->setBytes(1, &id_bytes);
        driver_statement->setString(2, addr);
        driver_statement->executeUpdate();
        std::unique_ptr<sql::PreparedStatement> scheduler_statement(m_conn->prepareStatement(
                "INSERT INTO schedulers (id, port, state) VALUES (?, ?, 'normal')"
        ));
        scheduler_statement->setBytes(1, &id_bytes);
        scheduler_statement->setInt(2, port);
        scheduler_statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

static std::string task_state_to_string(TaskState state) {
    if (state == kPending) {
        return "pending";
    } else if (state == kReady) {
        return "ready";
    } else if (state == kRunning) {
        return "running";
    } else if (state == kSucceed) {
        return "success";
    } else if (state == kFailed) {
        return "fail";
    } else if (state == kCanceled) {
        return "cancel";
    }
    return "";
}

static TaskState string_to_task_state(std::string const& state) {
    if (state == "pending") {
        return kPending;
    } else if (state == "ready") {
        return kReady;
    } else if (state == "running") {
        return kRunning;
    } else if (state == "success") {
        return kSucceed;
    } else if (state == "fail") {
        return kFailed;
    } else if (state == "cancel") {
        return kCanceled;
    }
    return kPending;
}

void MySqlMetadataStorage::add_task(sql::bytes job_id, Task const& task) {
    // Add task
    std::unique_ptr<sql::PreparedStatement> task_statement(m_conn->prepareStatement(
            "INSERT INTO tasks (id, job_id, func_name, state, timeout) VALUES (?, ?, ?, ?, ?)"
    ));
    sql::bytes task_id_bytes = uuid_get_bytes(task.get_id());
    task_statement->setBytes(1, &task_id_bytes);
    task_statement->setBytes(2, &job_id);
    task_statement->setString(3, task.get_function_name());
    task_statement->setString(4, task_state_to_string(task.get_state()));
    task_statement->setFloat(5, task.get_timeout());
    task_statement->executeUpdate();

    // Add task inputs
    for (uint64_t i = 0; i < task.get_num_inputs(); ++i) {
        TaskInput const input = task.get_input(i);
        if (input.get_task_output().has_value()) {
            std::tuple<boost::uuids::uuid, uint8_t> const task_output
                    = input.get_task_output().value();
            std::unique_ptr<sql::PreparedStatement> input_statement(m_conn->prepareStatement(
                    "INSERT INTO task_inputs (task_id, position, type, output_task_id, "
                    "output_task_position) VALUES (?, ?, ?, ?, ?)"
            ));
            input_statement->setBytes(1, &task_id_bytes);
            input_statement->setUInt(2, i);
            input_statement->setString(3, input.get_type());
            sql::bytes task_output_id = uuid_get_bytes(std::get<0>(task_output));
            input_statement->setBytes(4, &task_output_id);
            input_statement->setUInt(5, std::get<1>(task_output));
            input_statement->executeUpdate();
        } else if (input.get_data_id().has_value()) {
            std::unique_ptr<sql::PreparedStatement> input_statement(m_conn->prepareStatement(
                    "INSERT INTO task_inputs (task_id, position, type, data_id) VALUES (?, ?, ?, ?)"
            ));
            input_statement->setBytes(1, &task_id_bytes);
            input_statement->setUInt(2, i);
            input_statement->setString(3, input.get_type());
            sql::bytes data_id = uuid_get_bytes(input.get_data_id().value());
            input_statement->setBytes(4, &data_id);
            input_statement->executeUpdate();
        } else if (input.get_value().has_value()) {
            std::unique_ptr<sql::PreparedStatement> input_statement(m_conn->prepareStatement(
                    "INSERT INTO task_inputs (task_id, position, type, value) VALUES (?, ?, ?, ?)"
            ));
            input_statement->setBytes(1, &task_id_bytes);
            input_statement->setUInt(2, i);
            input_statement->setString(3, input.get_type());
            input_statement->setString(4, input.get_value().value());
            input_statement->executeUpdate();
        }
    }

    // Add task outputs
    for (uint64_t i = 0; i < task.get_num_outputs(); i++) {
        TaskOutput const output = task.get_output(i);
        std::unique_ptr<sql::PreparedStatement> output_statement(m_conn->prepareStatement(
                "INSERT INTO task_outputs (task_id, position, type) VALUES (?, ?, ?)"
        ));
        output_statement->setBytes(1, &task_id_bytes);
        output_statement->setUInt(2, i);
        output_statement->setString(3, output.get_type());
        output_statement->executeUpdate();
    }
}

StorageErr MySqlMetadataStorage::add_task_graph(TaskGraph const& task_graph) {
    try {
        sql::bytes const job_id_bytes = uuid_get_bytes(task_graph.get_id());

        // Tasks must be added in graph order to avoid the dangling reference.
        absl::flat_hash_set<boost::uuids::uuid> heads;
        std::deque<boost::uuids::uuid> queue;
        // First go ver all heads
        for (boost::uuids::uuid const task_id : heads) {
            Task const task = task_graph.get_task(task_id).value();
            this->add_task(job_id_bytes, task);
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
                Task const task = task_graph.get_task(task_id).value();
                this->add_task(job_id_bytes, task);
                for (boost::uuids::uuid const id : task_graph.get_child_tasks(task_id)) {
                    queue.push_back(id);
                }
            }
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

static boost::uuids::uuid read_id(std::istream* stream) {
    char id_bytes[16];
    stream->read(id_bytes, 16);
    std::uint8_t id_data[16];
    memcpy(id_data, id_bytes, 16);
    return {id_data};
}

Task MySqlMetadataStorage::fetch_task(std::shared_ptr<sql::ResultSet> res) {
    boost::uuids::uuid const id = read_id(res->getBinaryStream("id"));
    std::string const function_name = res->getString("func_name").c_str();
    TaskState const state = string_to_task_state(res->getString("state").c_str());
    boost::uuids::uuid const creator_id = read_id(res->getBinaryStream("creator"));
    float const timeout = res->getFloat("timeout");
    // Check creator type
    TaskCreatorType creator_type = kTask;
    std::unique_ptr<sql::PreparedStatement> driver_creator_statement(
            m_conn->prepareStatement("SELECT * FROM drivers WHERE id = ?")
    );
    sql::bytes id_bytes = uuid_get_bytes(creator_id);
    driver_creator_statement->setBytes(1, &id_bytes);
    std::unique_ptr<sql::ResultSet> driver_res(driver_creator_statement->executeQuery());
    if (driver_res->rowsCount() == 0) {
        creator_type = kClient;
    }
    return Task(id, function_name, state, creator_type, creator_id, timeout);
}

static void fetch_task_input(TaskGraph* task_graph, std::shared_ptr<sql::ResultSet> res) {
    boost::uuids::uuid const task_id = read_id(res->getBinaryStream(1));
    std::string const type = res->getString(3).c_str();
    if (!res->isNull(4)) {
        TaskInput input = TaskInput(read_id(res->getBinaryStream(4)), res->getUInt(5), type);
        if (!res->isNull(6)) {
            input.set_value(res->getString(6).c_str());
        }
        if (!res->isNull(7)) {
            input.set_data_id(read_id(res->getBinaryStream(7)));
        }
        task_graph->get_task(task_id)->add_input(input);
    } else if (!res->isNull(6)) {
        task_graph->get_task(task_id)->add_input(TaskInput(res->getString(6).c_str(), type));
    } else if (!res->isNull(7)) {
        task_graph->get_task(task_id)->add_input(TaskInput(read_id(res->getBinaryStream(7)), type));
    }
}

static void fetch_task_output(TaskGraph* task_graph, std::shared_ptr<sql::ResultSet> res) {
    boost::uuids::uuid const task_id = read_id(res->getBinaryStream(1));
    std::string const type = res->getString(3).c_str();
    TaskOutput output{type};
    if (!res->isNull(3)) {
        output.set_value(res->getString(3).c_str());
    } else if (!res->isNull(4)) {
        output.set_data_id(read_id(res->getBinaryStream(4)));
    }
    task_graph->get_task(task_id)->add_output(output);
}

StorageErr MySqlMetadataStorage::get_task_graph(boost::uuids::uuid id, TaskGraph* task_graph) {
    try {
        // Get all tasks
        std::unique_ptr<sql::PreparedStatement> task_statement(m_conn->prepareStatement(
                "SELECT id, func_name, state, creator, timeout FROM tasks WHERE job_id = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        task_statement->setBytes(1, &id_bytes);
        std::shared_ptr<sql::ResultSet> const task_res(task_statement->executeQuery());
        if (task_res->rowsCount() == 0) {
            m_conn->commit();
            return StorageErr(
                    StorageErrType::kKeyNotFound,
                    std::format("no task graph with id %s", boost::uuids::to_string(id))
            );
        }
        while (task_res->next()) {
            task_graph->add_task(this->fetch_task(task_res));
        }

        // Get inputs
        std::unique_ptr<sql::PreparedStatement> input_statement(m_conn->prepareStatement(
                "SELECT t1.task_id, t1.position, t1.type, t1.output_task_id, "
                "t1.output_task_position, t1.value, t1.data_id FROM task_inputs AS t1 JOIN tasks "
                "ON t1.task_id = tasks.id WHERE tasks.job_id = ? ORDER BY t1.task_id, t1.position"
        ));
        input_statement->setBytes(1, &id_bytes);
        std::shared_ptr<sql::ResultSet> const input_res(input_statement->executeQuery());
        while (input_res->next()) {
            fetch_task_input(task_graph, input_res);
        }

        // Get outputs
        std::unique_ptr<sql::PreparedStatement> output_statement(m_conn->prepareStatement(
                "SELECT t1.task_id, t1.position, t1.type, t1.value, t1.data_id FROM task_outputs "
                "AS t1 JOIN tasks ON t1.task_id = tasks.id WHERE tasks.job_id = ? ORDER BY "
                "t1.task_id, t1.position"
        ));
        output_statement->setBytes(1, &id_bytes);
        std::shared_ptr<sql::ResultSet> const output_res(output_statement->executeQuery());
        while (output_res->next()) {
            fetch_task_output(task_graph, output_res);
        }

        // Get dependencies
        std::unique_ptr<sql::PreparedStatement> dep_statement(m_conn->prepareStatement(
                "SELECT t1.parent, t1.child FROM task_dependencies AS t1 JOIN tasks ON t1.parent = "
                "tasks.id WHERE tasks.job_id = ?"
        ));
        dep_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> const dep_res(dep_statement->executeQuery());
        while (dep_res->next()) {
            task_graph->add_dependencies(
                    read_id(dep_res->getBinaryStream(1)),
                    read_id(dep_res->getBinaryStream(2))
            );
        }

    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_KEY_NOT_FOUND) {
            return StorageErr(StorageErrType::kKeyNotFound, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::get_task_graphs(std::vector<boost::uuids::uuid>* task_graphs) {
    try {
        std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
        std::unique_ptr<sql::ResultSet> const res(
                statement->executeQuery("SELECT DISTINCT job_id FROM tasks")
        );
        while (res->next()) {
            task_graphs->emplace_back(read_id(res->getBinaryStream(1)));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::remove_task_graph(boost::uuids::uuid id) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("DELETE FROM tasks WHERE job_id = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::add_child(boost::uuids::uuid parent_id, Task const& child) {
    try {
        sql::bytes const job_id = uuid_get_bytes(child.get_id());
        this->add_task(job_id, child);

        // Add dependencies
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "INSERT INTO task_dependencies (parent, child) VALUES (?, ?)"
        ));
        sql::bytes parent_id_bytes = uuid_get_bytes(parent_id);
        sql::bytes child_id_bytes = uuid_get_bytes(child.get_id());
        statement->setBytes(1, &parent_id_bytes);
        statement->setBytes(2, &child_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::get_task(boost::uuids::uuid id, Task* task) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "SELECT id, func_name, state, creator, timeout FROM tasks WHERE id = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::shared_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->commit();
            return StorageErr(
                    StorageErrType::kKeyNotFound,
                    std::format("no task with id %s", boost::uuids::to_string(id))
            );
        }
        *task = fetch_task(res);
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::get_ready_tasks(std::vector<Task>* tasks) {
    try {
        std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
        std::shared_ptr<sql::ResultSet> const res(statement->executeQuery(
                "SELECT id, func_name, state, creator, timeout FROM tasks WHERE state = 'ready'"
        ));
        while (res->next()) {
            tasks->emplace_back(fetch_task(res));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::set_task_state(boost::uuids::uuid id, TaskState state) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("UPDATE tasks SET state = ? WHERE id = ?")
        );
        statement->setString(1, task_state_to_string(state));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(2, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_KEY_NOT_FOUND) {
            return StorageErr(StorageErrType::kKeyNotFound, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::add_task_instance(TaskInstance const& instance) {
    try {
        std::shared_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("INSERT INTO task_instances (id, task_id, start_time) "
                                         "VALUES(?, ?, CURRENT_TIMESTAMP())")
        );
        sql::bytes id_bytes = uuid_get_bytes(instance.id);
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::task_finish(TaskInstance const& instance) {
    try {
        std::shared_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "UPDATE tasks (instance_id) VALUES (?) WHERE id = ? AND instance_id is NULL"
        ));
        sql::bytes id_bytes = uuid_get_bytes(instance.id);
        sql::bytes task_id_bytes = uuid_get_bytes(instance.task_id);
        statement->setBytes(1, &id_bytes);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::get_task_timeout(std::vector<TaskInstance>* tasks) {
    try {
        std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery(
                "SELECT t1.id, t1.task_id FROM task_instances as t1 JOIN tasks ON t1.task_id = "
                "tasks.id WHERE tasks.timeout > 0.0001 AND TIMESTAMPDIFF(MICROSECOND, "
                "t1.start_time, CURRENT_TIMESTAMP()) > tasks.timeout * 1000"
        ));
        while (res->next()) {
            tasks->emplace_back(
                    TaskInstance{read_id(res->getBinaryStream(1)), read_id(res->getBinaryStream(2))}
            );
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::get_child_task(boost::uuids::uuid id, Task* child) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "SELECT id, func_name, state, creator, timeout FROM tasks JOIN task_dependencies "
                "as t2 WHERE tasks.id = t2.child AND t2.parent = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::shared_ptr<sql::ResultSet> const res(statement->executeQuery());
        *child = fetch_task(res);
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::get_parent_tasks(boost::uuids::uuid id, std::vector<Task>* tasks) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "SELECT id, func_name, state, creator, timeout FROM tasks JOIN task_dependencies "
                "as t2 WHERE tasks.id = t2.parent AND t2.child = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::shared_ptr<sql::ResultSet> const res(statement->executeQuery());
        while (res->next()) {
            tasks->emplace_back(fetch_task(res));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::update_heartbeat(boost::uuids::uuid id) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "UPDATE divers SET (heartbeat) VALUES (CURRENT_TIMESTAMP()) WHERE id = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::shared_ptr<sql::ResultSet> const res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->commit();
            return StorageErr(
                    StorageErrType::kKeyNotFound,
                    std::format("no driver with id %s", boost::uuids::to_string(id))
            );
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr
MySqlMetadataStorage::heartbeat_timeout(float timeout, std::vector<boost::uuids::uuid>* ids) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("SELECT id FROM drivers WHERE TIMESTAMPDIFF(MICROSECOND, "
                                         "heartbeat, CURRENT_TIMESTAMP()) > ?")
        );
        statement->setFloat(1, timeout * 1000);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        while (res->next()) {
            ids->emplace_back(read_id(res->getBinaryStream("id")));
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MySqlMetadataStorage::get_scheduler_state(boost::uuids::uuid id, std::string* state) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("SELECT state FROM schedulers WHERE id = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->rollback();
            return StorageErr(
                    StorageErrType::kKeyNotFound,
                    std::format("no scheduler with id %s", boost::uuids::to_string(id))
            );
        }
        *state = res->getString(1).c_str();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr
MySqlMetadataStorage::set_scheduler_state(boost::uuids::uuid id, std::string const& state) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("UPDATE schedulers SET (state) VALUES (?) WHERE id = ?")
        );
        statement->setString(1, state);
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(2, &id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MysqlDataStorage::connect(std::string const& url) {
    if (nullptr == m_conn) {
        try {
            sql::Driver* driver = sql::mariadb::get_driver_instance();
            sql::Properties properties;
            m_conn = driver->connect(sql::SQLString(url), properties);
            m_conn->setAutoCommit(false);
        } catch (sql::SQLException& e) {
            return StorageErr(StorageErrType::kConnectionErr, e.what());
        }
    }
}

void MysqlDataStorage::close() {
    if (m_conn != nullptr) {
        m_conn->close();
        m_conn = nullptr;
    }
}

StorageErr MysqlDataStorage::initialize() {
    try {
        for (char const* create_table_str : cCreateDataStorage) {
            std::unique_ptr<sql::Statement> statement(m_conn->createStatement());
            statement->executeUpdate(create_table_str);
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_CANT_CREATE_TABLE) {
            return StorageErr(StorageErrType::kCreateStorageErr, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }

    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MysqlDataStorage::add_data(Data const& data) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "INSERT INTO data (id, key, value, hard_locality) VALUES(?, ?, ?, ?)"
        ));
        sql::bytes id_bytes = uuid_get_bytes(data.get_id());
        statement->setBytes(1, &id_bytes);
        if (data.get_key().has_value()) {
            statement->setString(2, data.get_key().value());
        } else {
            statement->setNull(2, sql::DataType::VARCHAR);
        }
        statement->setString(3, data.get_value());
        statement->setBoolean(4, data.is_hard_locality());
        statement->executeUpdate();

        for (std::string const& addr : data.get_locality()) {
            std::unique_ptr<sql::PreparedStatement> locality_statement(m_conn->prepareStatement(
                    "INSERT INTO data_locality (id, address) VALUES (?, ?)"
            ));
            locality_statement->setBytes(1, &id_bytes);
            locality_statement->setString(2, addr);
            locality_statement->executeUpdate();
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MysqlDataStorage::get_data(boost::uuids::uuid id, Data* data) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(m_conn->prepareStatement(
                "SELECT id, key, value, hard_locality FROM schedulers WHERE id = ?"
        ));
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> res(statement->executeQuery());
        if (res->rowsCount() == 0) {
            m_conn->rollback();
            return StorageErr(
                    StorageErrType::kKeyNotFound,
                    std::format("no data with id %s", boost::uuids::to_string(id))
            );
        }
        *data = Data(id, res->getString(2).c_str(), res->getString(3).c_str());
        data->set_hard_locality(res->getBoolean(4));

        std::unique_ptr<sql::PreparedStatement> locality_statement(
                m_conn->prepareStatement("SELECT addr FROM schedulers WHERE id = ?")
        );
        locality_statement->setBytes(1, &id_bytes);
        std::unique_ptr<sql::ResultSet> locality_res(locality_statement->executeQuery());
        std::vector<std::string> locality;
        while (res->next()) {
            locality.emplace_back(res->getString(1));
        }
        if (!locality.empty()) {
            data->set_locality(locality);
        }
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr MysqlDataStorage::add_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("INSERT INTO data_ref_task (id, task_id) VALUES(?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr
MysqlDataStorage::remove_task_reference(boost::uuids::uuid id, boost::uuids::uuid task_id) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("DELETE FROM data_ref_task WHERE id = ? AND task_id = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes task_id_bytes = uuid_get_bytes(task_id);
        statement->setBytes(2, &task_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr
MysqlDataStorage::add_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("INSERT INTO data_ref_driver (id, task_id) VALUES(?, ?)")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        if (e.getErrorCode() == ER_DUP_KEY || e.getErrorCode() == ER_DUP_ENTRY) {
            return StorageErr(StorageErrType::kDuplicateKey, e.what());
        }
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

StorageErr
MysqlDataStorage::remove_driver_reference(boost::uuids::uuid id, boost::uuids::uuid driver_id) {
    try {
        std::unique_ptr<sql::PreparedStatement> statement(
                m_conn->prepareStatement("DELETE FROM data_ref_driver WHERE id = ? AND task_id = ?")
        );
        sql::bytes id_bytes = uuid_get_bytes(id);
        statement->setBytes(1, &id_bytes);
        sql::bytes driver_id_bytes = uuid_get_bytes(driver_id);
        statement->setBytes(2, &driver_id_bytes);
        statement->executeUpdate();
    } catch (sql::SQLException& e) {
        m_conn->rollback();
        return StorageErr(StorageErrType::kOtherErr, e.what());
    }
    m_conn->commit();
    return StorageErr(StorageErrType::kSuccess, "");
}

}  // namespace spider::core

#pragma clang diagnostic pop
