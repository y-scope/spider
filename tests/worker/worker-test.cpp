#include <iostream>
#include <stdexcept>

#include "../../src/spider/worker/FunctionManager.hpp"

namespace {
auto sum_test(int const x, int const y) -> int {
    std::cerr << x << " + " << y << " = " << x + y << "\n";
    return x + y;
}

auto error_test(int const /*x*/) -> int {
    throw std::runtime_error("Simulated error in worker");
}
}  // namespace

// NOLINTBEGIN(cert-err58-cpp)
SPIDER_WORKER_REGISTER_TASK(sum_test);
SPIDER_WORKER_REGISTER_TASK(error_test);
// NOLINTEND(cert-err58-cpp)
