#ifndef SPIDER_CLIENT_ERROR_HPP
#define SPIDER_CLIENT_ERROR_HPP

#include <cstdint>
#include <string>

namespace spider {

class SpiderException : public std::exception {
public:
    enum class ErrType : std::uint8_t {
        StorageErr,
        DuplicateTask,
        TaskNotFound,
    };

private:
    ErrType m_type;
    std::string m_description;

public:
    SpiderException(ErrType type, std::string description)
            : m_type(type),
              m_description(std::move(description)) {}

    auto what() -> char const* { return m_description.c_str(); }

    auto get_type() -> ErrType { return m_type; }
};

}  // namespace spider

#endif  // SPIDER_CLIENT_ERROR_HPP
