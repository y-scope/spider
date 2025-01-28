#include "MySqlConnection.hpp"

#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <variant>

#include <mariadb/conncpp/Connection.hpp>
#include <mariadb/conncpp/Driver.hpp>
#include <mariadb/conncpp/Exception.hpp>
#include <mariadb/conncpp/SQLString.hpp>
#include <spdlog/spdlog.h>

#include "../core/Error.hpp"

namespace spider::core {

auto MySqlConnection::create(std::string const& url) -> std::variant<MySqlConnection, StorageErr> {
    // Parse jdbc url
    std::regex const url_regex(R"(jdbc:mariadb://[^?]+(\?user=([^&]*)(&password=([^&]*))?)?)");
    std::smatch match;
    if (false == std::regex_match(url, match, url_regex)) {
        return StorageErr{StorageErrType::OtherErr, "Invalid url"};
    }
    bool const credential = match[2].matched && match[4].matched;
    std::unique_ptr<sql::Connection> conn;
    try {
        sql::Driver* driver = sql::mariadb::get_driver_instance();
        if (credential) {
            conn = std::unique_ptr<sql::Connection>(
                    driver->connect(sql::SQLString(url), match[2].str(), match[4].str())
            );
        } else {
            conn = std::unique_ptr<sql::Connection>(
                    driver->connect(sql::SQLString(url), sql::Properties{})
            );
        }
        conn->setAutoCommit(false);
        return MySqlConnection{std::move(conn)};
    } catch (sql::SQLException& e) {
        return StorageErr{StorageErrType::ConnectionErr, e.what()};
    }
}

MySqlConnection::~MySqlConnection() {
    if (m_connection) {
        try {
            m_connection->close();
        } catch (sql::SQLException& e) {
            spdlog::warn("Failed to close connection: {}", e.what());
        }
        m_connection.reset();
    }
}

auto MySqlConnection::operator*() const -> sql::Connection& {
    return *m_connection;
}

auto MySqlConnection::operator->() const -> sql::Connection* {
    return &*m_connection;
}

}  // namespace spider::core
