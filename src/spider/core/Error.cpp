#include "Error.hpp"

#include <ystdlib/error_handling/ErrorCode.hpp>

using StorageErrorCategory = ystdlib::error_handling::ErrorCategory<spider::core::StorageErrType>;

template <>
auto StorageErrorCategory::name() const noexcept -> char const* {
    return "spider::core::StorageError";
}

template <>
auto StorageErrorCategory::message(spider::core::StorageErrType error) const -> std::string {
    switch (error) {
        case spider::core::StorageErrType::Success:
            return "No error";
        case spider::core::StorageErrType::ConnectionErr:
            return "Cannot connect to storage";
        case spider::core::StorageErrType::DbNotFound:
            return "Cannot find the database";
        case spider::core::StorageErrType::KeyNotFoundErr:
            return "Cannot find the key";
        case spider::core::StorageErrType::DuplicateKeyErr:
            return "Key already exists";
        case spider::core::StorageErrType::ConstraintViolationErr:
            return "Violate foreign key constraint";
        case spider::core::StorageErrType::DeadLockErr:
            return "Storage deadlock";
        case spider::core::StorageErrType::TaskLanguageErr:
            return "Task language not supported";
        case spider::core::StorageErrType::OtherErr:
            return "Other storage error";
        default:
            return "Unknown storage error";
    }
}
