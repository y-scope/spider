#include "../../src/spider/worker/FunctionManager.hpp"

#include <cstdlib>

inline auto sum_test(int const x, int const y) -> int {
    return x + y;
}

inline auto error_test(int const x) -> int {
    std::exit(1);
    return x;
}

REGISTER_TASK(sum_test);
REGISTER_TASK(error_test);
