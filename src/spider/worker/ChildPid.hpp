#ifndef SPIDER_WORKER_CHILDPID_HPP
#define SPIDER_WORKER_CHILDPID_HPP

#include <unistd.h>

#include <csignal>

namespace spider::core {
class ChildPid {
public:
    static auto get_instance() -> ChildPid&;

    [[nodiscard]] auto get_pid() const -> std::sig_atomic_t;

    auto set_pid(pid_t pid) -> void;

private:
    std::sig_atomic_t volatile m_pid{0};
};
}  // namespace spider::core

#endif
