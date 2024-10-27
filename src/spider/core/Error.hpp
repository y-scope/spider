#ifndef SPIDER_CORE_ERROR_HPP
#define SPIDER_CORE_ERROR_HPP

namespace spider::core {
enum class StorageErrType {
    kConnectionErr = 0,
    kDbNotFound,
    kKeyNotFoundErr,
    kDuplicateKeyErr,
    kConstraintViolationErr,
};

struct StorageErr {
    StorageErrType type;
    std::string description;
};

}  // namespace spider::core

#endif  // SPIDER_CORE_ERROR_HPP
