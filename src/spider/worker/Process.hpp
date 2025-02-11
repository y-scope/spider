#ifndef SPIDER_WORKER_PROCESS_HPP
#define SPIDER_WORKER_PROCESS_HPP

#include <unistd.h>

#include <optional>
#include <string>
#include <vector>

namespace spider::worker {

class Process {
public:
    static auto spawn(
            std::string const& executable,  // Using std::string for null termination
            std::vector<std::string> const& args,
            std::optional<int> in,
            std::optional<int> out,
            std::optional<int> err
    ) -> Process;

    /* Waits for the process to finish.
     * @return the process exit code.
     */
    [[nodiscard]] auto wait() const -> int;

    /*
     * Terminates the process.
     * Sends a SIGKILL signal to the process.
     */
    auto terminate() const -> void;

    // Delete copy constructor and assignment operator
    Process(Process const&) = delete;
    auto operator=(Process const&) -> Process& = delete;
    // Default move constructor and assignment operator
    Process(Process&&) = default;
    auto operator=(Process&&) -> Process& = default;
    ~Process() = default;

private:
    pid_t m_pid;

    explicit Process(pid_t const pid) : m_pid(pid) {}
};

}  // namespace spider::worker

#endif
