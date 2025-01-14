#include "tasks.hpp"

#include <cmath>

#include <spider/client/spider.hpp>

auto square(spider::TaskContext&, int value) -> int {
  return value * value;
}

auto square_root(spider::TaskContext&, int value) -> double {
  return std::sqrt(value);
}

auto sum(spider::TaskContext&, int x, int y) -> int {
    return x + y;
}

// Register the tasks
// NOLINTNEXTLINE(cert-err58-cpp)
SPIDER_REGISTER_TASK(square);
SPIDER_REGISTER_TASK(square_root);
SPIDER_REGISTER_TASK(sum);
