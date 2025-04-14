#include "ChildPid.hpp"

#include <csignal>

namespace spider::core {
auto ChildPid::get_pid() -> std::sig_atomic_t {
    return m_pid;
}

auto ChildPid::set_pid(pid_t const pid) -> void {
    m_pid = pid;
}

std::sig_atomic_t volatile ChildPid::m_pid = 0;
}  // namespace spider::core
