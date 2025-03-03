#ifndef SPIDER_CLIENT_DATA_HPP
#define SPIDER_CLIENT_DATA_HPP

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Error.hpp"
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MySqlConnection.hpp"
#include "Exception.hpp"

namespace spider {

namespace core {
class Data;
class DataStorage;
class DataImpl;
class TaskGraphImpl;
}  // namespace core
class Driver;
class TaskContext;

/**
 * A representation of data stored on external storage. This class allows the user to define:
 * - how the data should be cleaned up (garbage collected) once it is no longer referenced.
 * - the locality of the data.
 *
 * Example:
 * @code{.cpp}
 * auto disk_file_data = spider::Data<std::string>::Builder()
 *         .set_locality({"node_address"}, true)
 *         .set_cleanup_func([](std::string const& path) { std::filesystem::remove(path); })
 *         .build("/path/of/file");
 * @endcode
 *
 * @tparam T Type of the value.
 */
template <Serializable T>
class Data {
public:
    /**
     * @return The stored value.
     */
    auto get() -> T {
        std::string const& value = m_impl->get_value();
        return msgpack::unpack(value.data(), value.size()).get().as<T>();
    }

    /**
     * Sets the data's locality, indicated by the nodes that contain the data.
     *
     * @param nodes
     * @param hard Whether the data is only accessible from the given nodes (i.e., the locality is a
     * hard requirement).
     * @throw spider::ConnectionException
     */
    void set_locality(std::vector<std::string> const& nodes, bool hard) {
        m_impl->set_locality(nodes);
        m_impl->set_hard_locality(hard);
        std::variant<core::MySqlConnection, core::StorageErr> conn_result
                = core::MySqlConnection::create(m_data_store->get_url());
        if (std::holds_alternative<core::StorageErr>(conn_result)) {
            throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
        }
        auto& conn = std::get<core::MySqlConnection>(conn_result);
        m_data_store->set_data_locality(conn, *m_impl);
    }

    class Builder {
    public:
        /**
         * Sets the data's locality, indicated by the nodes that contain the data.
         *
         * @param nodes
         * @param hard Whether the data is only accessible from the given nodes (i.e., the locality
         * is a hard requirement.
         * @return self
         */
        auto set_locality(std::vector<std::string> const& nodes, bool hard) -> Builder& {
            m_nodes = nodes;
            m_hard_locality = hard;
            return *this;
        }

        /**
         * Sets the cleanup function for the data. This function will be called when the data is no
         * longer referenced.
         *
         * @param f
         * @return self
         */
        auto set_cleanup_func(std::function<void(T const&)> const& f) -> Builder& {
            m_cleanup_func = f;
            return *this;
        }

        /**
         * Builds the data object.
         *
         * @param t Value of the data
         * @return The built object.
         * @throw spider::ConnectionException
         */
        auto build(T const& t) -> Data {
            msgpack::sbuffer buffer;
            msgpack::pack(buffer, t);
            auto data = std::make_unique<core::Data>(std::string{buffer.data(), buffer.size()});
            data->set_locality(m_nodes);
            data->set_hard_locality(m_hard_locality);
            std::variant<core::MySqlConnection, core::StorageErr> conn_result
                    = core::MySqlConnection::create(m_data_store->get_url());
            if (std::holds_alternative<core::StorageErr>(conn_result)) {
                throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
            }
            auto& conn = std::get<core::MySqlConnection>(conn_result);
            core::StorageErr err;
            switch (m_data_source) {
                case DataSource::Driver:
                    err = m_data_store->add_driver_data(conn, m_source_id, *data);
                    if (!err.success()) {
                        throw ConnectionException(err.description);
                    }
                    break;
                case DataSource::TaskContext:
                    err = m_data_store->add_task_data(conn, m_source_id, *data);
                    if (!err.success()) {
                        throw ConnectionException(err.description);
                    }
                    break;
            }
            return Data{std::move(data), m_data_store};
        }

    private:
        enum class DataSource : std::uint8_t {
            Driver,
            TaskContext
        };

        Builder(std::shared_ptr<core::DataStorage> data_store,
                boost::uuids::uuid const source_id,
                DataSource const data_source)
                : m_data_store{std::move(data_store)},
                  m_source_id{source_id},
                  m_data_source{data_source} {}

        std::vector<std::string> m_nodes;
        bool m_hard_locality = false;
        std::function<void(T const&)> m_cleanup_func;

        std::shared_ptr<core::DataStorage> m_data_store;
        boost::uuids::uuid m_source_id;
        DataSource m_data_source;

        friend class Driver;
        friend class TaskContext;
    };

    Data() = default;

private:
    Data(std::unique_ptr<core::Data> impl, std::shared_ptr<core::DataStorage> data_store)
            : m_impl{std::move(impl)},
              m_data_store{std::move(data_store)} {}

    [[nodiscard]] auto get_impl() const -> std::unique_ptr<core::Data> const& { return m_impl; }

    std::unique_ptr<core::Data> m_impl;
    std::shared_ptr<core::DataStorage> m_data_store;

    friend class core::DataImpl;
    friend class core::TaskGraphImpl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_DATA_HPP
