#ifndef SPIDER_CLIENT_JOB_HPP
#define SPIDER_CLIENT_JOB_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "task.hpp"

namespace spider {
class JobImpl;

// TODO: Use std::expected or Boost's outcome so that the user can get the result of the job in one
// call rather than the current error-prone approach which requires that the user check the job's
// status and then call the relevant method.

enum class JobStatus : uint8_t {
    Running,
    Succeeded,
    Failed,
    Cancelled,
};

/**
 * A running task graph.
 *
 * @tparam ReturnType
 */
template <TaskIo ReturnType>
class Job {
public:
    /**
     * Waits for the job to complete.
     */
    auto wait_complete();

    /**
     * Cancels the job and waits for the all running tasks cancelled.
     */
    auto cancel();

    /**
     * @return Status of the job.
     */
    auto get_status() -> JobStatus;

    /**
     * NOTE: It is undefined behavior to call this method for a job that is not in the `Succeed`
     * state.
     *
     * @return Result of the job.
     */
    auto get_result() -> ReturnType;

    /**
     * NOTE: It is undefined behavior to call this method for a job that is not in the `Fail` state.
     *
     * @return A pair:
     * - the name of the task function that failed.
     * - the error message sent from the task through `TaskContext::abort` or from Spider.
     */
    auto get_error() -> std::pair<std::string, std::string>;

private:
    std::unique_ptr<JobImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_JOB_HPP
