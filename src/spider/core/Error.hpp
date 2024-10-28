#ifndef SPIDER_CORE_ERROR_HPP
#define SPIDER_CORE_ERROR_HPP

namespace spider::core {
enum class StorageErrType {
    kConnectionErr = 0,
    kCreateStorageErr,
    kStorageNotFound,
    kKeyNotFound,
    kDuplicateKey,
    kConstraintViolation,
    kOtherErr,
    kSuccess
};

struct StorageErr {
    StorageErrType type;
    std::string description;

    explicit operator bool() const { type == StorageErrType::kSuccess }
};

}  // namespace spider::core

#endif  // SPIDER_CORE_ERROR_HPP
