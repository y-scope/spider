#include "MySqlJobSubmissionBatch.hpp"

#include <mariadb/conncpp/Connection.hpp>
#include <mariadb/conncpp/Exception.hpp>
#include <mariadb/conncpp/PreparedStatement.hpp>

#include "../../core/Error.hpp"
#include "mysql_stmt.hpp"
#include "MySqlConnection.hpp"

namespace spider::core {

MySqlJobSubmissionBatch::MySqlJobSubmissionBatch(StorageConnection& conn)
        : m_job_stmt{static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertJob)},
          m_task_stmt{static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertTask)},
          m_task_input_output_stmt{static_cast<MySqlConnection&>(conn)->prepareStatement(
                  mysql::cInsertTaskInputOutput
          )},
          m_task_input_value_stmt{static_cast<MySqlConnection&>(conn)->prepareStatement(
                  mysql::cInsertTaskInputValue
          )},
          m_task_input_data_stmt{
                  static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertTaskInputData)
          },
          m_task_output_stmt{
                  static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertTaskOutput)
          },
          m_task_dependency_stmt{static_cast<MySqlConnection&>(conn)->prepareStatement(
                  mysql::cInsertTaskDependency
          )},
          m_input_task_stmt{
                  static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertInputTask)
          },
          m_output_task_stmt{
                  static_cast<MySqlConnection&>(conn)->prepareStatement(mysql::cInsertOutputTask)
          } {}

auto MySqlJobSubmissionBatch::submit_batch(StorageConnection& conn) -> StorageErr {
    try {
        m_job_stmt->executeBatch();
        m_task_stmt->executeBatch();
        m_task_output_stmt->executeBatch();  // Update task outputs in case of input reference
        m_task_input_output_stmt->executeBatch();
        m_task_input_value_stmt->executeBatch();
        m_task_input_data_stmt->executeBatch();
        m_task_dependency_stmt->executeBatch();
        m_input_task_stmt->executeBatch();
        m_output_task_stmt->executeBatch();
    } catch (sql::SQLException& e) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        static_cast<MySqlConnection&>(conn)->rollback();
        return StorageErr{StorageErrType::OtherErr, e.what()};
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    static_cast<MySqlConnection&>(conn)->commit();
    return StorageErr{};
}

}  // namespace spider::core
