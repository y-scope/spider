#include <iostream>
#include <stdexcept>

#include "../../src/spider/client/Data.hpp"
#include "../../src/spider/client/TaskContext.hpp"
#include "../../src/spider/worker/FunctionManager.hpp"

namespace {
auto sum_test(spider::TaskContext const& /*context*/, int const x, int const y) -> int {
    std::cerr << x << " + " << y << " = " << x + y << "\n";
    return x + y;
}

auto error_test(spider::TaskContext const& /*context*/, int const /*x*/) -> int {
    throw std::runtime_error("Simulated error in worker");
}

auto data_test(spider::TaskContext const& /*context*/, spider::Data<int>& data) -> int {
    return data.get();
}
}  // namespace

// NOLINTBEGIN(cert-err58-cpp)
SPIDER_WORKER_REGISTER_TASK(sum_test);
SPIDER_WORKER_REGISTER_TASK(error_test);
SPIDER_WORKER_REGISTER_TASK(data_test);
// NOLINTEND(cert-err58-cpp)
