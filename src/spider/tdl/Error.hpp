#ifndef SPIDER_TDL_ERROR_HPP
#define SPIDER_TDL_ERROR_HPP

#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <spider/tdl/parser/SourceLocation.hpp>

namespace spider::tdl {
/**
 * Represents a generic error in the TDL compiler.
 */
class Error {
public:
    // Constructor
    Error(std::string message,
          parser::SourceLocation source_location,
          std::optional<std::error_code> error_code = std::nullopt)
            : m_message{std::move(message)},
              m_source_location{source_location},
              m_error_code{error_code} {}

    // Methods
    [[nodiscard]] auto get_message() const noexcept -> std::string_view { return m_message; }

    [[nodiscard]] auto get_source_location() const noexcept -> parser::SourceLocation {
        return m_source_location;
    }

    [[nodiscard]] auto get_error_code() const noexcept -> std::optional<std::error_code> {
        return m_error_code;
    }

private:
    // Variables
    std::string m_message;
    parser::SourceLocation m_source_location;
    std::optional<std::error_code> m_error_code;
};
}  // namespace spider::tdl

#endif  // SPIDER_TDL_ERROR_HPP
