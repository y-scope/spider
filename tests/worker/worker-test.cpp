#include <iostream>
#include <random>
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

auto random_fail_test(spider::TaskContext const& /*context*/, int fail_rate) -> int {
    std::random_device rd;
    std::mt19937 gen{rd()};
    constexpr int cMaxFailRate = 100;
    std::uniform_int_distribution dis{1, cMaxFailRate};
    int const random_number = dis(gen);
    std::cerr << "Fail rate: " << fail_rate << "\n";
    std::cerr << "Random number: " << random_number << "\n";
    if (random_number < fail_rate) {
        throw std::runtime_error("Simulated error in worker");
    }
    return 0;
}
}  // namespace

// NOLINTBEGIN(cert-err58-cpp)
SPIDER_WORKER_REGISTER_TASK(sum_test);
SPIDER_WORKER_REGISTER_TASK(error_test);
SPIDER_WORKER_REGISTER_TASK(data_test);
SPIDER_WORKER_REGISTER_TASK(random_fail_test);
// NOLINTEND(cert-err58-cpp)
