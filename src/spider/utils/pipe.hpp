#ifndef SPIDER_UTILS_PIPE_HPP
#define SPIDER_UTILS_PIPE_HPP

#include <utility>

namespace spider::core {
/**
 * Creates a pipe.
 * @return A pair containing two file descriptors:
 * - The read end of the pipe.
 * - The write end of the pipe.
 * @throw std::runtime_error if the pipe creation fails.
 */
[[nodiscard]] auto create_pipe() -> std::pair<int, int>;
}  // namespace spider::core

#endif
