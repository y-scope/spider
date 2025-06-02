#ifndef SPIDER_WORKER_CHILDPID_HPP
#define SPIDER_WORKER_CHILDPID_HPP

#include <unistd.h>

#include <csignal>

namespace spider::core {
/**
 * @brief A singleton class to manage the child process ID for signal handler.
 *
 * User can set the child process ID using set_pid() method, and retrieve it using get_pid() method.
 * This class is signal-safe but is **not** thread-safe.
 */
class ChildPid {
public:
    /*
     * @return The process ID of the child process.
     */
    [[nodiscard]] static auto get_pid() -> std::sig_atomic_t;

    /*
     * @param pid The process ID to set.
     */
    static auto set_pid(pid_t pid) -> void;

    // Delete constructor
    ChildPid() = delete;
    // Delete copy constructor and assignment operator
    ChildPid(ChildPid const&) = delete;
    auto operator=(ChildPid const&) -> ChildPid& = delete;
    // Delete move constructor and assignment operator
    ChildPid(ChildPid&&) = delete;
    auto operator=(ChildPid&&) -> ChildPid& = delete;

    // Default destructor
    ~ChildPid() = default;

private:
    static std::sig_atomic_t volatile m_pid;
};
}  // namespace spider::core

#endif
