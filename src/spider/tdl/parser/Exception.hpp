#ifndef SPIDER_TDL_PARSER_EXCEPTION_HPP
#define SPIDER_TDL_PARSER_EXCEPTION_HPP

#include <exception>
#include <system_error>
#include <type_traits>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/Error.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>

namespace spider::tdl::parser {
class Exception : public std::exception {
public:
    // Static methods
    /**
     * @tparam T The expected value type on success.
     * @param result
     * @param source_location
     * @return The value forwarded from `result` on success.
     * @throws spider::tdl::parser::Exception if `result` indicates an error.
     */
    template <typename T>
    requires std::is_move_constructible_v<T>
    [[nodiscard]] static auto throw_tryx(
            ystdlib::error_handling::Result<T, std::error_code> result,
            SourceLocation source_location
    ) -> T {
        if (result.has_error()) {
            throw Exception{result.error(), source_location};
        }
        return std::move(result.value());
    }

    /**
     * @param result
     * @param source_location
     * @throws spider::tdl::parser::Exception if `result` indicates an error.
     */
    static auto throw_tryv(
            ystdlib::error_handling::Result<void, std::error_code> result,
            SourceLocation source_location
    ) -> void {
        if (result.has_error()) {
            throw Exception{result.error(), source_location};
        }
    }

    // Constructor
    Exception(std::error_code error_code, SourceLocation source_location)
            : m_error_code{error_code},
              m_source_location{source_location} {}

    // Methods implementing `std::exception`.
    [[nodiscard]] auto what() const noexcept -> char const* override {
        return "spider::tdl::parser::Exception";
    }

    // Methods
    [[nodiscard]] auto get_error_code() const noexcept -> std::error_code { return m_error_code; }

    [[nodiscard]] auto get_source_location() const noexcept -> SourceLocation {
        return m_source_location;
    }

    [[nodiscard]] auto to_error() const -> Error {
        return Error{std::string{what()}, m_source_location, m_error_code};
    }

private:
    // Variables
    std::error_code m_error_code;
    SourceLocation m_source_location;
};
}  // namespace spider::tdl::parser

#endif  // SPIDER_TDL_PARSER_EXCEPTION_HPP
