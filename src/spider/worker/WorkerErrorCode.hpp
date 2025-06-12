#ifndef SPIDER_WORKER_ERROR_CODE_HPP
#define SPIDER_WORKER_ERROR_CODE_HPP

#include <cstdint>

#include <ystdlib/error_handling/ErrorCode.hpp>

namespace spider::worker {
enum class WorkerErrorCodeEnum : uint8_t {
    Success = 0,
    CmdLineArgumentInvalid = 1,
    TaskArgumentInvalid = 2,
    TaskFailed = 3,
    TaskOutputUnavailable = 4,
    TaskOutputInvalid = 5,
    // TODO: Move storage related errors to an ErrorCode in the storage namespace.
    StorageError = 6,
};

using WorkerErrorCode = ystdlib::error_handling::ErrorCode<WorkerErrorCodeEnum>;
}  // namespace spider::worker

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(spider::worker::WorkerErrorCodeEnum);

#endif
