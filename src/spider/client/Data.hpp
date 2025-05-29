#ifndef SPIDER_CLIENT_DATA_HPP
#define SPIDER_CLIENT_DATA_HPP

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <spider/client/Exception.hpp>
#include <spider/core/Context.hpp>
#include <spider/core/DataCleaner.hpp>
#include <spider/core/Error.hpp>
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/io/Serializer.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>

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
        if (nullptr != m_connection) {
            m_data_store->set_data_locality(*m_connection, *m_impl);
            return;
        }
        std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                = m_storage_factory->provide_storage_connection();
        if (std::holds_alternative<core::StorageErr>(conn_result)) {
            throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
        }
        auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));
        m_data_store->set_data_locality(*conn, *m_impl);
    }

    /**
     * Sets the data as persisted, indicating the data should not be cleaned up.
     *
     * @throw spider::ConnectionException
     */
    void set_persisted() {
        m_impl->set_persisted(true);
        if (nullptr != m_connection) {
            m_data_store->set_data_persisted(*m_connection, *m_impl);
            return;
        }
        std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                = m_storage_factory->provide_storage_connection();
        if (std::holds_alternative<core::StorageErr>(conn_result)) {
            throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
        }
        auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));
        m_data_store->set_data_persisted(*conn, *m_impl);
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
         * Sets the data as persisted, indicating the data should not be cleaned up.
         *
         * @return self
         */
        auto set_persisted() -> Builder& {
            m_persisted = true;
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
            data->set_persisted(m_persisted);
            std::shared_ptr<core::StorageConnection> conn = m_connection;
            if (nullptr == conn) {
                std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                        = m_storage_factory->provide_storage_connection();
                if (std::holds_alternative<core::StorageErr>(conn_result)) {
                    throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
                }
                conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));
            }
            core::StorageErr err;
            switch (m_context.get_source()) {
                case core::Context::Source::Driver:
                    err = m_data_store->add_driver_data(*conn, m_context.get_id(), *data);
                    if (!err.success()) {
                        throw ConnectionException(err.description);
                    }
                    break;
                case core::Context::Source::Task:
                    err = m_data_store->add_task_data(*conn, m_context.get_id(), *data);
                    if (!err.success()) {
                        throw ConnectionException(err.description);
                    }
                    break;
            }
            return Data{std::move(data), m_context, m_data_store, m_storage_factory, m_connection};
        }

    private:
        Builder(core::Context context,
                std::shared_ptr<core::DataStorage> data_store,
                std::shared_ptr<core::StorageFactory> storage_factory)
                : m_context{context},
                  m_data_store{std::move(data_store)},
                  m_storage_factory{std::move(storage_factory)} {}

        Builder(core::Context context,
                std::shared_ptr<core::DataStorage> data_store,
                std::shared_ptr<core::StorageFactory> storage_factory,
                std::shared_ptr<core::StorageConnection> connection)
                : m_context{context},
                  m_data_store{std::move(data_store)},
                  m_storage_factory{std::move(storage_factory)},
                  m_connection{std::move(connection)} {}

        std::vector<std::string> m_nodes;
        bool m_hard_locality = false;
        std::function<void(T const&)> m_cleanup_func;
        bool m_persisted = false;

        std::shared_ptr<core::DataStorage> m_data_store;
        std::shared_ptr<core::StorageFactory> m_storage_factory;
        std::shared_ptr<core::StorageConnection> m_connection = nullptr;

        core::Context m_context;

        friend class Driver;
        friend class TaskContext;
    };

    Data() = default;

private:
    Data(std::unique_ptr<core::Data> impl,
         core::Context context,
         std::shared_ptr<core::DataStorage> data_store,
         std::shared_ptr<core::StorageFactory> storage_factory)
            : m_data_cleaner{std::make_unique<core::DataCleaner>(
                      impl->get_id(),
                      context,
                      data_store,
                      storage_factory,
                      nullptr
              )},
              m_impl{std::move(impl)},
              m_data_store{std::move(data_store)},
              m_storage_factory{std::move(storage_factory)} {}

    Data(std::unique_ptr<core::Data> impl,
         core::Context context,
         std::shared_ptr<core::DataStorage> data_store,
         std::shared_ptr<core::StorageFactory> storage_factory,
         std::shared_ptr<core::StorageConnection> connection)
            : m_data_cleaner{std::make_unique<core::DataCleaner>(
                      impl->get_id(),
                      context,
                      data_store,
                      storage_factory,
                      connection
              )},
              m_impl{std::move(impl)},
              m_data_store{std::move(data_store)},
              m_storage_factory{std::move(storage_factory)},
              m_connection{std::move(connection)} {}

    [[nodiscard]] auto get_impl() const -> std::unique_ptr<core::Data> const& { return m_impl; }

    std::unique_ptr<core::DataCleaner> m_data_cleaner;
    std::unique_ptr<core::Data> m_impl;
    std::shared_ptr<core::DataStorage> m_data_store;
    std::shared_ptr<core::StorageFactory> m_storage_factory;
    std::shared_ptr<core::StorageConnection> m_connection = nullptr;

    friend class core::DataImpl;
    friend class core::TaskGraphImpl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_DATA_HPP
