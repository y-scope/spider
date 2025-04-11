#include "StopToken.hpp"

namespace spider::core {
auto StopToken::get_instance() -> StopToken& {
    static StopToken instance;
    return instance;
}

auto StopToken::request_stop() -> void {
    m_stop = 1;
}

auto StopToken::stop_requested() const -> bool {
    return 0 != m_stop;
}

auto StopToken::reset() -> void {
    m_stop = 0;
}
}  // namespace spider::core
