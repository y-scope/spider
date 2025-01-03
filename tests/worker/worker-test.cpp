#include "worker-test.hpp"

#include <iostream>
#include <random>
#include <stdexcept>

#include "../../src/spider/client/Data.hpp"
#include "../../src/spider/client/Driver.hpp"
#include "../../src/spider/client/TaskContext.hpp"

auto sum_test(spider::TaskContext& /*context*/, int const x, int const y) -> int {
    std::cerr << x << " + " << y << " = " << x + y << "\n";
    return x + y;
}

auto error_test(spider::TaskContext& /*context*/, int const /*x*/) -> int {
    throw std::runtime_error("Simulated error in worker");
}

auto data_test(spider::TaskContext& /*context*/, spider::Data<int>& data) -> int {
    return data.get();
}

auto random_fail_test(spider::TaskContext& /*context*/, int fail_rate) -> int {
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

// NOLINTBEGIN(cert-err58-cpp)
SPIDER_REGISTER_TASK(sum_test);
SPIDER_REGISTER_TASK(error_test);
SPIDER_REGISTER_TASK(data_test);
SPIDER_REGISTER_TASK(random_fail_test);
// NOLINTEND(cert-err58-cpp)
