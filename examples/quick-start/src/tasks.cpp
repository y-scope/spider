#include "tasks.hpp"

#include <spider/client/spider.hpp>

// Task function implementation
auto sum(spider::TaskContext& /*context*/, int x, int y) -> int {
    return x + y;
}

// Register the task with Spider
SPIDER_REGISTER_TASK(sum);
