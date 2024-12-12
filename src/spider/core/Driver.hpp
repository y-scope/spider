#ifndef SPIDER_CORE_DRIVER_HPP
#define SPIDER_CORE_DRIVER_HPP

#include <string>

#include <boost/uuid/uuid.hpp>

namespace spider::core {

class Driver {
public:
    Driver(boost::uuids::uuid const id, std::string const& addr) : m_id{id}, m_addr{addr} {}

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid { return m_id; }

    [[nodiscard]] auto get_addr() const -> std::string { return m_addr; }

private:
    boost::uuids::uuid m_id;
    std::string m_addr;
};

class Scheduler {
public:
    Scheduler(boost::uuids::uuid const id, std::string const& addr, int port)
            : m_id{id},
              m_addr{addr},
              m_port{port} {}

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid { return m_id; }

    [[nodiscard]] auto get_addr() const -> std::string { return m_addr; }

    [[nodiscard]] auto get_port() const -> int { return m_port; }

private:
    boost::uuids::uuid m_id;
    std::string m_addr;
    int m_port;
};

}  // namespace spider::core

#endif  // SPIDER_CORE_DRIVER_HPP
