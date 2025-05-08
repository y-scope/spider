#ifndef SPIDER_CORE_CONTEXT_HPP
#define SPIDER_CORE_CONTEXT_HPP

#include <cstdint>

#include <boost/uuid/uuid.hpp>

namespace spider::core {
/**
 *  Context class wraps the information about the execution context.
 *  Context could be either a driver (i.e. client) or a task.
 */
class Context {
public:
    enum class Source : uint8_t {
        Driver,
        Task,
    };

    Context(Source const source, boost::uuids::uuid const id) : m_source{source}, m_id{id} {}

    [[nodiscard]] auto get_source() const -> Source { return m_source; }

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid { return m_id; }

private:
    Source m_source;
    boost::uuids::uuid m_id;
};
}  // namespace spider::core

#endif
