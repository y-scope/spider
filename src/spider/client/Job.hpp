#ifndef SPIDER_CLIENT_JOB_HPP
#define SPIDER_CLIENT_JOB_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace spider {
class JobImpl;

enum class JobStatus : uint8_t {
    Running,
    Succeed,
    Fail,
    Cancel,
};

/**
 * Job represents a running task graph.
 *
 * @tparam T output type of the job.
 */
template <class T>
class Job {
public:
    /**
     * Waits for the job to complete.
     */
    auto wait_complete();

    /**
     * Gets the status of the job.
     *
     * @return Status of the job.
     */
    auto get_status() -> JobStatus;

    /**
     * Get the result of the succeeded job.
     * It is undefined behavior to call on job that is in other status.
     *
     * @return Result of the job.
     */
    auto get_result() -> T;

    /**
     * Get the error message of the failed job.
     * It is undefined behavior to call on job that is in other status.
     *
     * @return first is the function that fails. second is the error message provided in
     * context::abort
     */
    auto get_error() -> std::pair<std::string, std::string>;

private:
    std::unique_ptr<JobImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_JOB_HPP
