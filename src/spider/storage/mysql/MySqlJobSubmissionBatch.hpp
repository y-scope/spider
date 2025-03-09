#ifndef SPIDER_STORAGE_MYSQLJOBSUBMISSIONBATCH_HPP
#define SPIDER_STORAGE_MYSQLJOBSUBMISSIONBATCH_HPP

#include <memory>

#include <mariadb/conncpp/Connection.hpp>
#include <mariadb/conncpp/PreparedStatement.hpp>

#include "../JobSubmissionBatch.hpp"
#include "mysql_stmt.hpp"
#include "MySqlConnection.hpp"

namespace spider::core {
class MySqlJobSubmissionBatch : public JobSubmissionBatch {
public:
    explicit MySqlJobSubmissionBatch(sql::Connection& conn)
            : m_job_stmt{conn.prepareStatement(mysql::cInsertJob)},
              m_task_stmt{conn.prepareStatement(mysql::cInsertTask)},
              m_task_input_output_stmt{conn.prepareStatement(mysql::cInsertTaskInputOutput)},
              m_task_input_value_stmt{conn.prepareStatement(mysql::cInsertTaskInputValue)},
              m_task_input_data_stmt{conn.prepareStatement(mysql::cInsertTaskInputData)},
              m_task_output_stmt{conn.prepareStatement(mysql::cInsertTaskOutput)},
              m_task_dependency_stmt{conn.prepareStatement(mysql::cInsertTaskDependency)},
              m_input_task_stmt{conn.prepareStatement(mysql::cInsertInputTask)},
              m_output_task_stmt{conn.prepareStatement(mysql::cInsertOutputTask)} {}

    explicit MySqlJobSubmissionBatch(MySqlConnection& conn)
            : m_job_stmt{conn->prepareStatement(mysql::cInsertJob)},
              m_task_stmt{conn->prepareStatement(mysql::cInsertTask)},
              m_task_input_output_stmt{conn->prepareStatement(mysql::cInsertTaskInputOutput)},
              m_task_input_value_stmt{conn->prepareStatement(mysql::cInsertTaskInputValue)},
              m_task_input_data_stmt{conn->prepareStatement(mysql::cInsertTaskInputData)},
              m_task_output_stmt{conn->prepareStatement(mysql::cInsertTaskOutput)},
              m_task_dependency_stmt{conn->prepareStatement(mysql::cInsertTaskDependency)},
              m_input_task_stmt{conn->prepareStatement(mysql::cInsertInputTask)},
              m_output_task_stmt{conn->prepareStatement(mysql::cInsertOutputTask)} {}

    auto submit_batch() -> void {
        m_job_stmt->executeBatch();
        m_task_stmt->executeBatch();
        m_task_output_stmt->executeBatch();  // Update task outputs in case of input reference
        m_task_input_output_stmt->executeBatch();
        m_task_input_value_stmt->executeBatch();
        m_task_input_data_stmt->executeBatch();
        m_task_dependency_stmt->executeBatch();
        m_input_task_stmt->executeBatch();
        m_output_task_stmt->executeBatch();
    }

    auto get_job_stmt() -> sql::PreparedStatement& { return *m_job_stmt; }

    auto get_task_stmt() -> sql::PreparedStatement& { return *m_task_stmt; }

    auto get_task_input_output_stmt() -> sql::PreparedStatement& {
        return *m_task_input_output_stmt;
    }

    auto get_task_input_value_stmt() -> sql::PreparedStatement& { return *m_task_input_value_stmt; }

    auto get_task_input_data_stmt() -> sql::PreparedStatement& { return *m_task_input_data_stmt; }

    auto get_task_output_stmt() -> sql::PreparedStatement& { return *m_task_output_stmt; }

    auto get_task_dependency_stmt() -> sql::PreparedStatement& { return *m_task_dependency_stmt; }

    auto get_input_task_stmt() -> sql::PreparedStatement& { return *m_input_task_stmt; }

    auto get_output_task_stmt() -> sql::PreparedStatement& { return *m_output_task_stmt; }

private:
    std::unique_ptr<sql::PreparedStatement> m_job_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_input_output_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_input_value_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_input_data_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_output_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_dependency_stmt;
    std::unique_ptr<sql::PreparedStatement> m_input_task_stmt;
    std::unique_ptr<sql::PreparedStatement> m_output_task_stmt;
};
}  // namespace spider::core

#endif
