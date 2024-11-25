#ifndef SPIDER_CLIENT_EXCEPTION_HPP
#define SPIDER_CLIENT_EXCEPTION_HPP

#include <cstdint>
#include <exception>

namespace spider {
enum class ExceptionCode : std::uint8_t {
    ConnectionError,
};

struct SpiderException : std::exception {
    ExceptionCode code;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_EXCEPTION_HPP
