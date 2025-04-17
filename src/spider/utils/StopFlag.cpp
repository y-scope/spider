#include "StopFlag.hpp"

#include <atomic>

namespace spider::core {
auto StopFlag::request_stop() -> void {
    m_stop.test_and_set();
}

auto StopFlag::is_stop_requested() -> bool {
    return m_stop.test();
}

auto StopFlag::reset() -> void {
    m_stop.clear();
}

std::atomic_flag StopFlag::m_stop = ATOMIC_FLAG_INIT;
}  // namespace spider::core
