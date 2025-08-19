#include "WorkerErrorCode.hpp"

#include <string>
#include <string_view>

#include <ystdlib/error_handling/ErrorCode.hpp>

namespace spider::worker {
using WorkerErrorCategory = ystdlib::error_handling::ErrorCategory<WorkerErrorCodeEnum>;

constexpr std::string_view cWorkerErrorCategoryName = "Worker Error Code";
}  // namespace spider::worker

template <>
auto spider::worker::WorkerErrorCategory::name() const noexcept -> char const* {
    return spider::worker::cWorkerErrorCategoryName.data();
}

template <>
auto spider::worker::WorkerErrorCategory::message(spider::worker::WorkerErrorCodeEnum code) const
        -> std::string {
    switch (code) {
        case spider::worker::WorkerErrorCodeEnum::Success:
            return "Success";
        case spider::worker::WorkerErrorCodeEnum::CmdLineArgumentInvalid:
            return "Invalid command line argument";
        case spider::worker::WorkerErrorCodeEnum::TaskArgumentInvalid:
            return "Invalid task argument";
        case spider::worker::WorkerErrorCodeEnum::TaskFailed:
            return "Task failed";
        case spider::worker::WorkerErrorCodeEnum::TaskOutputUnavailable:
            return "Task output unavailable";
        case spider::worker::WorkerErrorCodeEnum::TaskOutputInvalid:
            return "Task output invalid";
        case spider::worker::WorkerErrorCodeEnum::StorageError:
            return "Storage error";
        default:
            return "Unknown error";
    }
}
