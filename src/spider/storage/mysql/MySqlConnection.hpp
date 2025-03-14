#ifndef SPIDER_STORAGE_MYSQLCONNECTION_HPP
#define SPIDER_STORAGE_MYSQLCONNECTION_HPP

#include <memory>
#include <string>
#include <utility>
#include <variant>

#include <mariadb/conncpp/Connection.hpp>

#include "../../core/Error.hpp"
#include "../StorageConnection.hpp"

namespace spider::core {

// Forward declaration for friend class
class MySqlStorageFactory;

// RAII class for MySQL connection
class MySqlConnection : public StorageConnection {
public:
    // Delete copy constructor and copy assignment operator
    MySqlConnection(MySqlConnection const&) = delete;
    auto operator=(MySqlConnection const&) -> MySqlConnection& = delete;
    // Default move constructor and move assignment operator
    MySqlConnection(MySqlConnection&&) = default;
    auto operator=(MySqlConnection&&) -> MySqlConnection& = default;

    ~MySqlConnection() override;

    auto operator*() const -> sql::Connection&;
    auto operator->() const -> sql::Connection*;

private:
    static auto create(std::string const& url
    ) -> std::variant<std::unique_ptr<StorageConnection>, StorageErr>;

    explicit MySqlConnection(std::unique_ptr<sql::Connection> conn)
            : m_connection{std::move(conn)} {};
    std::unique_ptr<sql::Connection> m_connection;

    friend class MySqlStorageFactory;
};

}  // namespace spider::core

#endif
