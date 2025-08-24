#include "pipe.hpp"

#include <unistd.h>

#include <array>
#include <stdexcept>
#include <utility>

namespace spider::core {
auto create_pipe() -> std::pair<int, int> {
    std::array<int, 2> pipe_fds{};
    if (pipe(pipe_fds.data()) == -1) {
        throw std::runtime_error("Failed to create pipe");
    }
    return {pipe_fds[0], pipe_fds[1]};
}
}  // namespace spider::core
