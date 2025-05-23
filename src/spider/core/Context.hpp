#ifndef SPIDER_CORE_CONTEXT_HPP
#define SPIDER_CORE_CONTEXT_HPP

#include <cstdint>

#include <boost/uuid/uuid.hpp>

namespace spider::core {
/**
 * Represents the execution context in which operations are performed.
 *
 * The Context class encapsulates information about whether the current
 * execution is occurring in a client (e.g. driver) or within a task.
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
