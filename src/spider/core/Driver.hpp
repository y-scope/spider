#ifndef SPIDER_CORE_DRIVER_HPP
#define SPIDER_CORE_DRIVER_HPP

#include <string>
#include <utility>

#include <boost/uuid/uuid.hpp>

namespace spider::core {
class Driver {
public:
    explicit Driver(boost::uuids::uuid const id) : m_id{id} {}

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid const& { return m_id; }

private:
    boost::uuids::uuid m_id;
};

class Scheduler {
public:
    Scheduler(boost::uuids::uuid const id, std::string addr, int port)
            : m_id{id},
              m_addr{std::move(addr)},
              m_port{port} {}

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid const& { return m_id; }

    [[nodiscard]] auto get_addr() const -> std::string const& { return m_addr; }

    [[nodiscard]] auto get_port() const -> int { return m_port; }

private:
    boost::uuids::uuid m_id;
    std::string m_addr;
    int m_port;
};
}  // namespace spider::core

#endif  // SPIDER_CORE_DRIVER_HPP
