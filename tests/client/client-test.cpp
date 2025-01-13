#include <string>
#include <tuple>

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

    // Run a complicated graph that should succeed
    spider::TaskGraph const left = driver.bind(&sum_test, &data_test, &data_test);
    spider::TaskGraph const graph = driver.bind(&sum_test, left, &sum_test);
    spdlog::debug("Graph created");
    spider::Data<int> d1 = driver.get_data_builder<int>().build(1);
    spider::Data<int> d2 = driver.get_data_builder<int>().build(2);
    spdlog::debug("Data created");
    spider::Job<int> job = driver.start(graph, d1, d2, 3, 4);
    spdlog::debug("Job started");
    job.wait_complete();
    spdlog::debug("Job completed");
    if (job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Job failed");
        return cJobFailed;
    }
    constexpr int cExpectedResult = 10;
    if (job.get_result() != cExpectedResult) {
        spdlog::error("Wrong job result. Get {}. Expect 10", job.get_result());
        return cJobFailed;
    }

    // Run task with multiple results should succeed
    spider::Job<std::tuple<int, int>> swap_job = driver.start(&swap_test, 1, 2);
    spdlog::debug("Multiple result job started");
    swap_job.wait_complete();
    if (swap_job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Multiple result job failed");
        return cJobFailed;
    }
    std::tuple<int, int> swap_result = swap_job.get_result();
    if (std::get<0>(swap_result) != 2 || std::get<1>(swap_result) != 1) {
        spdlog::error(
                "Wrong multiple result job result. Get ({}, {}). Expect (2, 1)",
                std::get<0>(swap_result),
                std::get<1>(swap_result)
        );
        return cJobFailed;
    }

    // Run fail job
    spider::Job fail_job = driver.start(&error_test, 1);
    spdlog::debug("Fail job started");
    fail_job.wait_complete();
    spdlog::debug("Fail job completed");
    if (fail_job.get_status() != spider::JobStatus::Failed) {
        spdlog::error("Job should fail");
        return cJobFailed;
    }

    // Run random fail job
    constexpr int cFailRate = 5;
    spider::Job random_fail_job = driver.start(&random_fail_test, cFailRate);
    spdlog::debug("Random fail job started");
    random_fail_job.wait_complete();
    spdlog::debug("Random fail job completed");
    if (random_fail_job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Random fail job failed");
        return cJobFailed;
    }

    // Run task that creates data
    spider::Job create_data_job = driver.start(&create_data_test, 1);
    spdlog::debug("Create data job started");
    create_data_job.wait_complete();
    spdlog::debug("Create data job completed");
    if (create_data_job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Create data job failed");
        return cJobFailed;
    }
    spider::Data<int> data_result = create_data_job.get_result();
    if (data_result.get() != 1) {
        spdlog::error("Create data job failed");
        return cJobFailed;
    }

    // Run task that creates task
    spider::Job create_task_job = driver.start(&create_task_test, 1, 2);
    spdlog::debug("Create task job started");
    create_task_job.wait_complete();
    spdlog::debug("Create task job completed");
    if (create_task_job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Create task job failed");
        return cJobFailed;
    }
    if (create_task_job.get_result() != 3) {
        spdlog::error("Create task job failed");
        return cJobFailed;
    }

    return 0;
}
