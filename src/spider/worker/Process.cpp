#include "Process.hpp"

#include <dirent.h>
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <signal.h>
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace spider::worker {
namespace {
auto close_all_fds(std::vector<int> const& whitelist) -> bool {
    std::unique_ptr<DIR, void (*)(DIR*)> const dir{opendir("/dev/fd"), [](DIR* p) { closedir(p); }};
    if (nullptr == dir) {
        return false;
    }

    int const dir_fd = dirfd(dir.get());
    if (dir_fd == -1) {
        return false;
    }

    dirent* entry = nullptr;
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    while (nullptr != (entry = readdir(dir.get()))) {
        if (entry->d_type != DT_LNK) {
            continue;
        }
        // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        int const fd = std::strtol(entry->d_name, nullptr, 10);
        if (fd == 0 && (entry->d_name[0] != '0' && entry->d_name[1] != '\0')) {
            continue;
        }
        if (fd == dir_fd || fd == STDIN_FILENO || fd == STDOUT_FILENO || fd == STDERR_FILENO
            || std::ranges::find(whitelist, fd) != whitelist.end())
        {
            continue;
        }
        close(fd);
    }

    return true;
}
}  // namespace

auto Process::spawn(
        std::string const& executable,
        std::vector<std::string> const& args,
        std::optional<int> const in,
        std::optional<int> const out,
        std::optional<int> const err,
        std::vector<int> const& fd_whitelist
) -> Process {
    // Build execvp arguments
    std::vector<char*> exec_args;
    exec_args.reserve(args.size() + 2);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    exec_args.push_back(const_cast<char*>(executable.data()));
    for (std::string const& arg : args) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
        exec_args.push_back(const_cast<char*>(arg.data()));
    }
    exec_args.push_back(nullptr);

    pid_t const pid = fork();
    if (pid < 0) {
        throw std::runtime_error("Failed to fork process");
    }
    if (pid == 0) {
        // Child process
        if (in.has_value()) {
            dup2(in.value(), STDIN_FILENO);
        }
        if (out.has_value()) {
            dup2(out.value(), STDOUT_FILENO);
        }
        if (err.has_value()) {
            dup2(err.value(), STDERR_FILENO);
        }

        // Close all file descriptors except for stdin, stdout, and stderr
        if (false == close_all_fds(fd_whitelist)) {
            _exit(EXIT_FAILURE);
        }

        execvp(executable.c_str(), exec_args.data());
        _exit(EXIT_FAILURE);  // exec never returns
    }

    // Parent process
    return Process(pid);
}

constexpr int cSignalOffset = 128;

auto Process::wait() const -> int {
    int status = 0;
    if (waitpid(m_pid, &status, 0) == -1) {
        throw std::runtime_error("Failed to wait for process");
    }
    if (WIFSIGNALED(status)) {
        return cSignalOffset + WTERMSIG(status);
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return -1;  // Process did not exit normally
}

auto Process::terminate() const -> void {
    if (kill(m_pid, SIGKILL) == -1) {
        throw std::runtime_error("Failed to terminate process");
    }
}

auto Process::get_pid() const -> pid_t {
    return m_pid;
}
}  // namespace spider::worker
