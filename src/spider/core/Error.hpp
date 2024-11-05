#ifndef SPIDER_CORE_ERROR_HPP
#define SPIDER_CORE_ERROR_HPP

#include <cstdint>
#include <string>
#include <utility>

namespace spider::core {
enum class StorageErrType : std::uint8_t {
    Success = 0,
    ConnectionErr,
    DbNotFound,
    KeyNotFoundErr,
    DuplicateKeyErr,
    ConstraintViolationErr,
    OtherErr
};

struct StorageErr {
    StorageErrType type;
    std::string description;

    StorageErr() : type(StorageErrType::Success) {}

    StorageErr(StorageErrType type, std::string description)
            : type(type),
              description(std::move(description)) {}

    auto success() const -> bool { return StorageErrType::Success == type; }
};

}  // namespace spider::core

#endif  // SPIDER_CORE_ERROR_HPP
