#ifndef SPIDER_UTILS_PIPE_HPP
#define SPIDER_UTILS_PIPE_HPP

#include <utility>

namespace spider::core {
auto create_pipe() -> std::pair<int, int>;
}  // namespace spider::core

#endif
