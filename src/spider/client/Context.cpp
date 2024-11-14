#include "Context.hpp"

#include <boost/uuid/uuid.hpp>

namespace spider {

class ContextImpl {
public:
    [[nodiscard]] auto get_id() const -> boost::uuids::uuid { return m_id; }

private:
    boost::uuids::uuid m_id;
};

auto Context::get_id() const -> boost::uuids::uuid {
    return m_impl->get_id();
}

}  // namespace spider
