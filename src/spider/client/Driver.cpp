#include "Driver.hpp"

#include <chrono>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>

#include "../core/Driver.hpp"
#include "../core/Error.hpp"
#include "../core/KeyValueData.hpp"
#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../storage/MysqlStorage.hpp"
#include "Exception.hpp"

namespace spider {

Driver::Driver(std::string const& storage_url) {
    boost::uuids::random_generator gen;
    m_id = gen();

    m_metadata_storage = std::make_shared<core::MySqlMetadataStorage>();
    m_data_storage = std::make_shared<core::MySqlDataStorage>();
    core::StorageErr err = m_metadata_storage->connect(storage_url);
    if (!err.success()) {
        throw ConnectionException(err.description);
    }
    err = m_data_storage->connect(storage_url);
    if (!err.success()) {
        throw ConnectionException(err.description);
    }

    std::optional<std::string> const optional_addr = core::get_address();
    if (!optional_addr.has_value()) {
        throw ConnectionException("Cannot get machine address");
    }
    std::string const& addr = optional_addr.value();
    err = m_metadata_storage->add_driver(core::Driver{m_id, addr});
    if (!err.success()) {
        if (core::StorageErrType::DuplicateKeyErr == err.type) {
            throw DriverIdInUseException(m_id);
        }
        throw ConnectionException(err.description);
    }

    // Start a thread to send heartbeats
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    m_heartbeat_thread = std::jthread([this](std::stop_token stoken) {
        while (!stoken.stop_requested()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            core::StorageErr const err = m_metadata_storage->update_heartbeat(m_id);
            if (!err.success()) {
                throw ConnectionException(err.description);
            }
        }
    });
}

Driver::Driver(std::string const& storage_url, boost::uuids::uuid const id) : m_id{id} {
    m_metadata_storage = std::make_shared<core::MySqlMetadataStorage>();
    m_data_storage = std::make_shared<core::MySqlDataStorage>();
    core::StorageErr err = m_metadata_storage->connect(storage_url);
    if (!err.success()) {
        throw ConnectionException(err.description);
    }
    err = m_data_storage->connect(storage_url);
    if (!err.success()) {
        throw ConnectionException(err.description);
    }

    std::optional<std::string> const optional_addr = core::get_address();
    if (!optional_addr.has_value()) {
        throw ConnectionException("Cannot get machine address");
    }
    std::string const& addr = optional_addr.value();
    err = m_metadata_storage->add_driver(core::Driver{m_id, addr});
    if (!err.success()) {
        if (core::StorageErrType::DuplicateKeyErr == err.type) {
            throw DriverIdInUseException(m_id);
        }
        throw ConnectionException(err.description);
    }

    // Start a thread to send heartbeats
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    m_heartbeat_thread = std::jthread([this](std::stop_token stoken) {
        while (!stoken.stop_requested()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            core::StorageErr const err = m_metadata_storage->update_heartbeat(m_id);
            if (!err.success()) {
                throw ConnectionException(err.description);
            }
        }
    });
}

auto Driver::kv_store_insert(std::string const& key, std::string const& value) -> void {
    core::KeyValueData const kv_data{key, value, m_id};
    core::StorageErr const err = m_data_storage->add_client_kv_data(kv_data);
    if (!err.success()) {
        throw ConnectionException(err.description);
    }
}

auto Driver::kv_store_get(std::string const& key) -> std::optional<std::string> {
    std::string value;
    core::StorageErr const err = m_data_storage->get_client_kv_data(m_id, key, &value);
    if (!err.success()) {
        if (core::StorageErrType::KeyNotFoundErr == err.type) {
            return std::nullopt;
        }
        throw ConnectionException(err.description);
    }
    return value;
}

}  // namespace spider
