#include "ChildPid.hpp"

#include <unistd.h>

#include <csignal>

namespace spider::core {
auto ChildPid::get_instance() -> ChildPid& {
    return m_instance;
}

auto ChildPid::get_pid() const -> std::sig_atomic_t {
    return m_pid;
}

auto ChildPid::set_pid(pid_t const pid) -> void {
    m_pid = pid;
}
}  // namespace spider::core
