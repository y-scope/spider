#include "worker-test.hpp"

#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>

#include <spider/client/Data.hpp>
#include <spider/client/Driver.hpp>
#include <spider/client/Job.hpp>
#include <spider/client/TaskContext.hpp>
#include <spider/client/TaskGraph.hpp>

auto sum_test(spider::TaskContext& /*context*/, int const x, int const y) -> int {
    std::cerr << x << " + " << y << " = " << x + y << "\n";
    return x + y;
}

auto swap_test(spider::TaskContext& /*context*/, int const x, int const y) -> std::tuple<int, int> {
    return std::make_tuple(y, x);
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

auto create_data_test(spider::TaskContext& context, int x) -> spider::Data<int> {
    spider::Data<int> data = context.get_data_builder<int>().build(x);
    return data;
}

auto create_task_test(spider::TaskContext& context, int x, int y) -> int {
    spider::TaskGraph const graph = context.bind(&sum_test, &sum_test, 0);
    std::cerr << "Create task test\n";
    spider::Job job = context.start(graph, x, y);
    std::cerr << "Job started\n";
    job.wait_complete();
    std::cerr << "Job completed\n";
    if (job.get_status() != spider::JobStatus::Succeeded) {
        std::cerr << "Job failed\n";
        throw std::runtime_error("Job failed");
    }
    return job.get_result();
}

auto join_string_test(
        spider::TaskContext& /*context*/,
        std::string const& input_1,
        std::string const& input_2
) -> std::string {
    return input_1 + input_2;
}

// NOLINTBEGIN(cert-err58-cpp)
SPIDER_REGISTER_TASK(sum_test);
SPIDER_REGISTER_TASK(swap_test);
SPIDER_REGISTER_TASK(error_test);
SPIDER_REGISTER_TASK(data_test);
SPIDER_REGISTER_TASK(random_fail_test);
SPIDER_REGISTER_TASK(create_data_test);
SPIDER_REGISTER_TASK(create_task_test);
SPIDER_REGISTER_TASK(join_string_test);
// NOLINTEND(cert-err58-cpp)
