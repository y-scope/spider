#ifndef SPIDER_CLIENT_DATA_HPP
#define SPIDER_CLIENT_DATA_HPP

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../io/Serializer.hpp"
#include "../storage/DataStorage.hpp"

namespace spider {

namespace core {
class Data;
class DataStorage;
}  // namespace core

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
    auto get() -> T;

    /**
     * Sets the data's locality, indicated by the nodes that contain the data.
     *
     * @param nodes
     * @param hard Whether the data is only accessible from the given nodes (i.e., the locality is a
     * hard requirement).
     * @throw spider::ConnectionException
     */
    void set_locality(std::vector<std::string> const& nodes, bool hard);

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
        auto set_locality(std::vector<std::string> const& nodes, bool hard) -> Builder&;

        /**
         * Sets the cleanup function for the data. This function will be called when the data is no
         * longer referenced.
         *
         * @param f
         * @return self
         */
        auto set_cleanup_func(std::function<void(T const&)> const& f) -> Builder&;

        /**
         * Builds the data object.
         *
         * @param t Value of the data
         * @return The built object.
         * @throw spider::ConnectionException
         */
        auto build(T const& t) -> Data;

    private:
        enum class DataSource {
            Driver,
            TaskContext
        };
        explicit Builder(
                std::shared_ptr<core::DataStorage> data_store,
                boost::uuids::uuid const source_id,
                DataSource const data_source
        )
                : m_data_store{std::move(data_store)},
                  m_source_id{source_id},
                  m_data_source{data_source} {};

        std::vector<std::string> m_nodes;
        bool m_hard_locality = false;
        std::function<void(T const&)> m_cleanup_func;

        std::shared_ptr<core::DataStorage> m_data_store;
        boost::uuids::uuid m_source_id;
        DataSource m_data_source;

        friend class Driver;
        friend class TaskContext;
    };

private:
    Data(std::unique_ptr<core::Data> impl, std::shared_ptr<core::DataStorage> data_store)
            : m_impl{std::move(impl)},
              m_data_store{std::move(data_store)} {}

    [[nodiscard]] auto get_impl() const -> std::unique_ptr<core::Data> const& { return m_impl; }

    std::unique_ptr<core::Data> m_impl;
    std::shared_ptr<core::DataStorage> m_data_store;

    friend class msgpack::adaptor::pack<Data>;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_DATA_HPP
