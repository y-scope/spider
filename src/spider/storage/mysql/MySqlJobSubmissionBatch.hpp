#ifndef SPIDER_STORAGE_MYSQLJOBSUBMISSIONBATCH_HPP
#define SPIDER_STORAGE_MYSQLJOBSUBMISSIONBATCH_HPP

#include <memory>

#include <mariadb/conncpp/PreparedStatement.hpp>

#include "../../core/Error.hpp"
#include "../JobSubmissionBatch.hpp"
#include "../StorageConnection.hpp"
#include "MySqlStorageFactory.hpp"

namespace spider::core {

// Forward declaration for friend class
class MySqlStorageFactory;

class MySqlJobSubmissionBatch : public JobSubmissionBatch {
public:
    auto submit_batch(StorageConnection& conn) -> StorageErr override;

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
    explicit MySqlJobSubmissionBatch(StorageConnection& conn);

    std::unique_ptr<sql::PreparedStatement> m_job_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_input_output_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_input_value_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_input_data_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_output_stmt;
    std::unique_ptr<sql::PreparedStatement> m_task_dependency_stmt;
    std::unique_ptr<sql::PreparedStatement> m_input_task_stmt;
    std::unique_ptr<sql::PreparedStatement> m_output_task_stmt;

    friend class MySqlStorageFactory;
};
}  // namespace spider::core

#endif
