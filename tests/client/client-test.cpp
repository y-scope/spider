#include <boost/any/bad_any_cast.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>  // IWYU pragma: keep
#include <spdlog/spdlog.h>

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

    // Run a complicated graph that should succeed
    spider::TaskGraph left = driver.bind(&sum_test, &data_test, &data_test);
    spider::TaskGraph graph = driver.bind(&sum_test, left, &sum_test);
    spider::Data<int> d1 = driver.get_data_builder<int>().build(1);
    spider::Data<int> d2 = driver.get_data_builder<int>().build(2);
    spider::Job<int> job = driver.start(graph, d1, d2, 3, 4);
    job.wait_complete();
    if (job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Job failed");
        return cJobFailed;
    }
    if (job.get_result() != 10) {
        spdlog::error("Wrong job result. Get {}. Expect 10", job.get_result());
        return cJobFailed;
    }

    // Run fail job
    spider::Job fail_job = driver.start(&error_test, 1);
    fail_job.wait_complete();
    if (fail_job.get_status() != spider::JobStatus::Failed) {
        spdlog::error("Job should fail");
        return cJobFailed;
    }

    // Run random fail job
    constexpr int fail_rate = 5;
    spider::Job random_fail_job = driver.start(&random_fail_test, fail_rate);
    random_fail_job.wait_complete();
    if (random_fail_job.get_status() != spider::JobStatus::Succeeded) {
        spdlog::error("Random fail job failed");
        return cJobFailed;
    }

    return 0;
}
