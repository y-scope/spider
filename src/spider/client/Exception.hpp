#ifndef SPIDER_CLIENT_EXCEPTION_HPP
#define SPIDER_CLIENT_EXCEPTION_HPP

#include <exception>
#include <string>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>

namespace spider {
class ConnectionException final : std::exception {
public:
    auto what() -> std::string { return fmt::format("Cannot connect to storage {}.", m_addr); }

private:
    std::string m_addr;
};

class DriverIdUsedException final : std::exception {
public:
    auto what() -> std::string {
        return fmt::format("Driver id {} already used.", boost::uuids::to_string(m_id));
    }

private:
    boost::uuids::uuid m_id;
};

}  // namespace spider

#endif  // SPIDER_CLIENT_EXCEPTION_HPP
