#ifndef SPIDER_CLIENT_EXCEPTION_HPP
#define SPIDER_CLIENT_EXCEPTION_HPP

#include <exception>
#include <string>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>

namespace spider {
class ConnectionException final : public std::exception {
public:
    explicit ConnectionException(std::string const& addr)
            : m_message(fmt::format("Cannot connect to storage {}.", addr)) {}

    [[nodiscard]] auto what() const noexcept -> char const* override { return m_message.c_str(); }

private:
    std::string m_message;
};

class DriverIdInUseException final : public std::exception {
public:
    explicit DriverIdInUseException(boost::uuids::uuid id)
            : m_message(
                      fmt::format("Driver ID {} is currently in use.", boost::uuids::to_string(id))
              ) {}

    [[nodiscard]] auto what() const noexcept -> char const* override { return m_message.c_str(); }

private:
    std::string m_message;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_EXCEPTION_HPP
