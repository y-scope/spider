#include <chrono>
#include <exception>
#include <string>
#include <thread>
#include <utility>

#include <boost/any/bad_any_cast.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>  // IWYU pragma: keep
#include <spdlog/spdlog.h>

#include <spider/client/Driver.hpp>
#include <spider/client/Job.hpp>
#include <tests/worker/worker-test.hpp>

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
constexpr int cJobNotCancelled = 2;
constexpr int cWrongErrorMessage = 3;
constexpr int cException = 4;
}  // namespace

auto main(int argc, char** argv) -> int {
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [spider.client] %v");
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

    try {
        // Create driver
        spider::Driver driver{storage_url};
        spdlog::debug("Driver created");

        spider::Job<int> sleep_job = driver.start(&sleep_test, 3);

        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_job.cancel();

        sleep_job.wait_complete();
        if (spider::JobStatus::Cancelled != sleep_job.get_status()) {
            spdlog::error("Sleep job status is not cancelled");
            return cJobNotCancelled;
        }

        // Cancel task from task
        spider::Job<int> abort_job = driver.start(&abort_test, 0);
        abort_job.wait_complete();
        if (spider::JobStatus::Cancelled != abort_job.get_status()) {
            spdlog::error("Abort job status is not cancelled");
            return cJobNotCancelled;
        }
        std::pair<std::string, std::string> const job_errors = abort_job.get_error();
        if ("abort_test" != job_errors.first) {
            spdlog::error("Cancelled task wrong function name");
            return cWrongErrorMessage;
        }
        if ("Abort test" != job_errors.second) {
            spdlog::error("Cancelled task wrong error message");
            return cWrongErrorMessage;
        }
    } catch (std::exception& e) {
        spdlog::error("Exception: {}", e.what());
        return cException;
    }

    return 0;
}
