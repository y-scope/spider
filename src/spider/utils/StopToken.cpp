#include "StopToken.hpp"

namespace spider::core {
auto StopToken::get_instance() -> StopToken& {
    return m_instance;
}

auto StopToken::request_stop() -> void {
    m_stop.test_and_set();
}

auto StopToken::stop_requested() const -> bool {
    return m_stop.test();
}

auto StopToken::reset() -> void {
    m_stop.clear();
}
}  // namespace spider::core
