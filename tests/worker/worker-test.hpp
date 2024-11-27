#ifndef SPIDER_TEST_WORKER_WORKERTEST_HPP
#define SPIDER_TEST_WORKER_WORKERTEST_HPP

#include "../../src/spider/worker/FunctionManager.hpp"

inline auto sum_test(int const x, int const y) -> int {
    return x + y;
}

inline auto error_test() {
    exit(1);
}

REGISTER_TASK(sum_test);
REGISTER_TASK(error_test);

#endif  // SPIDER_TEST_WORKER_WORKERTEST_HPP
