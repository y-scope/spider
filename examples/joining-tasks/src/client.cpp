#include <iostream>
#include <string>
#include <type_traits>
#include <utility>

#include <spider/client/spider.hpp>

#include "tasks.hpp"

namespace {
/**
 * @tparam JobOutputType
 * @param job
 * @param expected
 * @return Whether the job was successful.
 */
template <typename JobOutputType>
auto validate_job_output(spider::Job<JobOutputType>& job, JobOutputType const& expected) -> bool {
    switch (auto job_status = job.get_status()) {
        case spider::JobStatus::Succeeded: {
            auto result = job.get_result();
            if (expected == result) {
                return true;
            }
            std::cerr << "job returned unexpected result. Expected: " << expected
                      << ". Actual: " << result << '\n';
            return false;
        }
        case spider::JobStatus::Failed: {
            std::pair<std::string, std::string> const error_and_fn_name = job.get_error();
            std::cerr << "Job failed in function " << error_and_fn_name.second << " - "
                      << error_and_fn_name.first << '\n';
            return false;
        }
        default:
            std::cerr << "Job is in unexpected state - "
                      << static_cast<std::underlying_type_t<decltype(job_status)>>(job_status)
                      << '\n';
            return false;
    }
}
}

auto main(int argc, char const* argv[]) -> int {
    // Parse the storage backend URL from the command line arguments
    if (argc < 2) {
        std::cerr << "Usage: ./client <storage-backend-url>" << '\n';
        return 1;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::string const storage_url{argv[1]};
    if (storage_url.empty()) {
        std::cerr << "storage-backend-url cannot be empty." << '\n';
        return 1;
    }

    // Create a driver that connects to the Spider cluster
    spider::Driver driver{storage_url};

    auto sum_of_squares_task_graph = driver.bind(&sum, &square, &square);
    auto hypotenuse_task_graph = driver.bind(&square_root, &sum_of_squares_task_graph);

    // Submit the task graph for execution
    constexpr int a = 4;
    constexpr int b = 5;
    auto job = driver.start(hypotenuse_task_graph, a, b);

    job.wait_complete();

    if (false == validate_job_output(job, std::sqrt(a * a + b * b))) {
        return 1;
    }

    return 0;
}
