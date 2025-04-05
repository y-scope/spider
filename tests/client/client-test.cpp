#include <cstddef>
#include <string>
#include <tuple>
#include <vector>

#include <boost/any/bad_any_cast.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>  // IWYU pragma: keep
#include <spdlog/spdlog.h>

#include "../../src/spider/client/Data.hpp"
#include "../../src/spider/client/Driver.hpp"
#include "../../src/spider/client/Job.hpp"
#include "../../src/spider/client/TaskGraph.hpp"
#include "../worker/worker-test.hpp"

namespace {
auto parse_args(int const argc, char** argv) -> boost::program_options::variables_map {
    boost::program_options::options_description desc;
    desc.add_options()("help", "spider client test");
    desc.add_options()(
            "storage_url",
            boost::program_options::value<std::string>(),
            "storage server url"
    );

    boost::program_options::variables_map variables;
    boost::program_options::store(
            // NOLINTNEXTLINE(misc-include-cleaner)
            boost::program_options::parse_command_line(argc, argv, desc),
            variables
    );
    boost::program_options::notify(variables);
    return variables;
}

constexpr int cCmdArgParseErr = 1;
constexpr int cJobFailed = 2;

constexpr int cBatchSize = 10;

auto test_graph(spider::Driver& driver) -> int {
    spider::TaskGraph const left = driver.bind(&sum_test, &data_test, &data_test);
    spider::TaskGraph const graph = driver.bind(&sum_test, left, &sum_test);
    spdlog::debug("Graph created");
    spider::Data<int> d1 = driver.get_data_builder<int>().build(1);
    spider::Data<int> d2 = driver.get_data_builder<int>().build(2);
    spdlog::debug("Data created");
    spider::Job<int> graph_job = driver.start(graph, d1, d2, 3, 4);
    spdlog::debug("Job started");
    graph_job.wait_complete();
    spdlog::debug("Job completed");
    if (graph_job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Job failed");
        return cJobFailed;
    }
    constexpr int cExpectedResult = 10;
    if (graph_job.get_result() != cExpectedResult) {
        spdlog::error("Wrong job result. Get {}. Expect 10", graph_job.get_result());
        return cJobFailed;
    }
    return 0;
}

auto test_multi_result(spider::Driver& driver) -> int {
    spider::Job<std::tuple<int, int>> job = driver.start(&swap_test, 1, 2);
    spdlog::debug("Multiple result job started");
    job.wait_complete();
    if (job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Multiple result job failed");
        return cJobFailed;
    }
    std::tuple<int, int> swap_result = job.get_result();
    if (std::get<0>(swap_result) != 2 || std::get<1>(swap_result) != 1) {
        spdlog::error(
                "Wrong multiple result job result. Get ({}, {}). Expect (2, 1)",
                std::get<0>(swap_result),
                std::get<1>(swap_result)
        );
        return cJobFailed;
    }
    return 0;
}

auto test_fail_job(spider::Driver& driver) -> int {
    spider::Job fail_job = driver.start(&error_test, 1);
    spdlog::debug("Fail job started");
    fail_job.wait_complete();
    spdlog::debug("Fail job completed");
    if (fail_job.get_status() != spider::JobStatus::Failed) {
        spdlog::error("Job should fail");
        return cJobFailed;
    }
    return 0;
}

auto test_task_create_data(spider::Driver& driver) -> int {
    spider::Job job = driver.start(&create_data_test, 1);
    spdlog::debug("Create data job started");
    job.wait_complete();
    spdlog::debug("Create data job completed");
    if (job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Create data job failed");
        return cJobFailed;
    }
    spider::Data<int> data_result = job.get_result();
    if (data_result.get() != 1) {
        spdlog::error("Create data job failed");
        return cJobFailed;
    }
    return 0;
}

auto test_task_create_task(spider::Driver& driver) -> int {
    spider::Job job = driver.start(&create_task_test, 1, 2);
    spdlog::debug("Create task job started");
    job.wait_complete();
    spdlog::debug("Create task job completed");
    if (job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Create task job failed");
        return cJobFailed;
    }
    if (job.get_result() != 3) {
        spdlog::error("Create task job failed");
        return cJobFailed;
    }
    return 0;
}

auto test_function_batch_submission(spider::Driver& driver) -> int {
    std::vector<spider::Job<int>> jobs;
    jobs.reserve(cBatchSize);
    driver.begin_batch_start();
    for (int i = 0; i < cBatchSize; ++i) {
        jobs.emplace_back(driver.start(&sum_test, i, i));
    }
    driver.end_batch_start();
    for (int i = 0; i < cBatchSize; ++i) {
        spider::Job<int>& job = jobs[i];
        job.wait_complete();
        if (job.get_status() != spider::JobStatus::Succeeded) {
            spdlog::error("Batch job failed");
            return cJobFailed;
        }
        int const result = job.get_result();
        if (result != i + i) {
            spdlog::error("Batch job wrong result. Expect {}. Get {}.", i + i, result);
            return cJobFailed;
        }
    }
    return 0;
}

auto test_graph_batch_submission(spider::Driver& driver) -> int {
    std::vector<spider::Job<int>> jobs;
    jobs.reserve(cBatchSize);
    driver.begin_batch_start();
    spider::TaskGraph const graph = driver.bind(&sum_test, &sum_test, &sum_test);
    for (int i = 0; i < cBatchSize; ++i) {
        jobs.emplace_back(driver.start(graph, i, i, i, i));
    }
    driver.end_batch_start();
    for (int i = 0; i < cBatchSize; ++i) {
        spider::Job<int>& job = jobs[i];
        job.wait_complete();
        if (job.get_status() != spider::JobStatus::Succeeded) {
            spdlog::error("Batch graph job failed");
            return cJobFailed;
        }
        int const result = job.get_result();
        if (result != i * 4) {
            spdlog::error("Batch job wrong result. Expect {}. Get {}.", i * 4, result);
            return cJobFailed;
        }
    }
    return 0;
}

auto test_large_input_output(
        spider::Driver& driver,
        size_t const input_size_1,
        size_t const input_size_2
) -> int {
    std::string input_1(input_size_1, 'a');
    std::string input_2(input_size_2, 'b');

    spider::Job<std::string> job = driver.start(&join_string_test, input_1, input_2);
    job.wait_complete();
    if (job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Large input job failed");
        return cJobFailed;
    }
    if (job.get_result() != input_1 + input_2) {
        spdlog::error(
                "Large input job wrong result. Expect {}. Get {}.",
                input_1 + input_2,
                job.get_result()
        );
        return cJobFailed;
    }
    return 0;
}

constexpr size_t cLargeInputSize = 300;
}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
auto main(int argc, char** argv) -> int {
    // NOLINTNEXTLINE(misc-include-cleaner)
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [spider.scheduler] %v");
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#endif

    boost::program_options::variables_map const args = parse_args(argc, argv);

    std::string storage_url;
    try {
        if (!args.contains("storage_url")) {
            spdlog::error("storage_url is required");
            return cCmdArgParseErr;
        }
        storage_url = args["storage_url"].as<std::string>();
    } catch (boost::bad_any_cast& e) {
        return cCmdArgParseErr;
    } catch (boost::program_options::error& e) {
        return cCmdArgParseErr;
    }

    // Create driver
    spider::Driver driver{storage_url};
    spdlog::debug("Driver created");

    int result = test_graph(driver);
    if (0 != result) {
        return result;
    }

    result = test_multi_result(driver);
    if (0 != result) {
        return result;
    }

    result = test_fail_job(driver);
    if (0 != result) {
        return result;
    }

    result = test_task_create_data(driver);
    if (0 != result) {
        return result;
    }

    result = test_task_create_task(driver);
    if (0 != result) {
        return result;
    }

    result = test_function_batch_submission(driver);
    if (0 != result) {
        return result;
    }

    result = test_graph_batch_submission(driver);
    if (0 != result) {
        return result;
    }

    result = test_large_input_output(driver, cLargeInputSize, cLargeInputSize);
    if (0 != result) {
        return result;
    }

    return 0;
}
