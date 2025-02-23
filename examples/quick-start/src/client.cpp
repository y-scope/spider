#include <iostream>
#include <string>
#include <type_traits>
#include <utility>

#include <spider/client/spider.hpp>

#include "tasks.hpp"

// NOLINTBEGIN(bugprone-exception-escape)
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

    // Submit the task for execution
    int const x = 2;
    int const y = 3;
    spider::Job<int> job = driver.start(&sum, x, y);

    // Wait for the job to complete
    job.wait_complete();

    // Handle the job's success/failure
    switch (auto job_status = job.get_status()) {
        case spider::JobStatus::Succeeded: {
            auto result = job.get_result();
            int const expected = x + y;
            if (expected == result) {
                return 0;
            }
            std::cerr << "`sum` returned unexpected result. Expected: " << expected
                      << ". Actual: " << result << '\n';
            return 1;
        }
        case spider::JobStatus::Failed: {
            std::pair<std::string, std::string> const error_and_fn_name = job.get_error();
            std::cerr << "Job failed in function " << error_and_fn_name.second << " - "
                      << error_and_fn_name.first << '\n';
            return 1;
        }
        default:
            std::cerr << "Job is in unexpected state - "
                      << static_cast<std::underlying_type_t<decltype(job_status)>>(job_status)
                      << '\n';
            return 1;
    }
}

// NOLINTEND(bugprone-exception-escape)
