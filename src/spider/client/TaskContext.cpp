#include "TaskContext.hpp"

#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Error.hpp"
#include "../core/KeyValueData.hpp"
#include "Exception.hpp"

namespace spider {

auto TaskContext::get_id() const -> boost::uuids::uuid {
    return m_task_id;
}

auto TaskContext::kv_store_get(std::string const& key) -> std::optional<std::string> {
    std::string value;
    core::StorageErr const err = m_data_store->get_task_kv_data(m_task_id, key, &value);
    if (!err.success()) {
        if (core::StorageErrType::KeyNotFoundErr == err.type) {
            return std::nullopt;
        }
        throw ConnectionException(err.description);
    }
    return value;
}

auto TaskContext::kv_store_insert(std::string const& key, std::string const& value) -> void {
    core::KeyValueData const kv_data{key, value, m_task_id};
    core::StorageErr const err = m_data_store->add_task_kv_data(kv_data);
    if (!err.success()) {
        throw ConnectionException(err.description);
    }
}

auto TaskContext::get_jobs() -> std::vector<boost::uuids::uuid> {
    std::vector<boost::uuids::uuid> job_ids;
    core::StorageErr const err = m_metadata_store->get_jobs_by_client_id(m_task_id, &job_ids);
    if (!err.success()) {
        throw ConnectionException("Failed to get jobs.");
    }
    return job_ids;
}

}  // namespace spider
