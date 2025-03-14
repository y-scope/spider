#include "TaskContext.hpp"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Error.hpp"
#include "../core/KeyValueData.hpp"
#include "../storage/StorageConnection.hpp"
#include "Exception.hpp"

namespace spider {

auto TaskContext::get_id() const -> boost::uuids::uuid {
    return m_task_id;
}

auto TaskContext::kv_store_get(std::string const& key) -> std::optional<std::string> {
    std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
            = m_storage_factory->provide_storage_connection();
    if (std::holds_alternative<core::StorageErr>(conn_result)) {
        throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
    }
    auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));

    std::string value;
    core::StorageErr const err = m_data_store->get_task_kv_data(*conn, m_task_id, key, &value);
    if (!err.success()) {
        if (core::StorageErrType::KeyNotFoundErr == err.type) {
            return std::nullopt;
        }
        throw ConnectionException(err.description);
    }
    return value;
}

auto TaskContext::kv_store_insert(std::string const& key, std::string const& value) -> void {
    std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
            = m_storage_factory->provide_storage_connection();
    if (std::holds_alternative<core::StorageErr>(conn_result)) {
        throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
    }
    auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));

    core::KeyValueData const kv_data{key, value, m_task_id};
    core::StorageErr const err = m_data_store->add_task_kv_data(*conn, kv_data);
    if (!err.success()) {
        throw ConnectionException(err.description);
    }
}

auto TaskContext::get_jobs() -> std::vector<boost::uuids::uuid> {
    std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
            = m_storage_factory->provide_storage_connection();
    if (std::holds_alternative<core::StorageErr>(conn_result)) {
        throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
    }
    auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));

    std::vector<boost::uuids::uuid> job_ids;
    core::StorageErr const err
            = m_metadata_store->get_jobs_by_client_id(*conn, m_task_id, &job_ids);
    if (!err.success()) {
        throw ConnectionException("Failed to get jobs.");
    }
    return job_ids;
}

}  // namespace spider
