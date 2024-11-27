#include "../../src/spider/worker/FunctionManager.hpp"

auto sum_test(int const x, int const y) -> int {
    return x + y;
}

auto error_test() -> int {
    exit(1);
    return 1;
}

REGISTER_TASK(sum_test);
REGISTER_TASK(error_test);
