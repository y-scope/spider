#include "StopToken.hpp"

#include <atomic>

namespace spider::core {
auto StopToken::request_stop() -> void {
    m_stop.test_and_set();
}

auto StopToken::is_stop_requested() -> bool {
    return m_stop.test();
}

auto StopToken::reset() -> void {
    m_stop.clear();
}

std::atomic_flag StopToken::m_stop = ATOMIC_FLAG_INIT;
}  // namespace spider::core
