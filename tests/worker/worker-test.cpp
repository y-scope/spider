#include "../../src/spider/worker/FunctionManager.hpp"

#include <cstdlib>

namespace {
auto sum_test(int const x, int const y) -> int {
    return x + y;
}

auto error_test(int const x) -> int {
    std::quick_exit(1);
    return x;
}
}  // namespace

// NOLINTBEGIN(cert-err58-cpp)
REGISTER_TASK(sum_test);
REGISTER_TASK(error_test);
// NOLINTEND(cert-err58-cpp)
