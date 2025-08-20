#ifndef SPIDER_TDL_PARSER_ERRORLISTENER_HPP
#define SPIDER_TDL_PARSER_ERRORLISTENER_HPP

#include <cstddef>
#include <exception>
#include <optional>
#include <string>
#include <utility>

#include <antlr4-runtime.h>
#include <fmt/format.h>

#include <spider/tdl/Error.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>

namespace spider::tdl::parser {
class ErrorListener : public antlr4::BaseErrorListener {
public:
    // Constructor
    explicit ErrorListener(std::string tag) : m_tag{std::move(tag)} {}

    // Methods implementing `antlr4::BaseErrorListener`
    auto syntaxError(
            [[maybe_unused]] antlr4::Recognizer* recognizer,
            [[maybe_unused]] antlr4::Token* offending_symbol,
            size_t line,
            size_t char_position_in_line,
            std::string const& msg,
            [[maybe_unused]] std::exception_ptr e
    ) -> void override {
        m_error.emplace(
                fmt::format("{}: {}", m_tag, msg),
                SourceLocation{line, char_position_in_line},
                std::nullopt
        );
    }

    // Methods
    [[nodiscard]] auto has_error() const -> bool { return m_error.has_value(); }

    /**
     * @return A reference to the error. The caller must ensure that `has_error()` is true before
     * calling this method.
     */
    [[nodiscard]] auto error() const -> Error const& {
        // We require the caller to check `has_error()` before calling this method, which ensures
        // the optional var has a value.
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        return m_error.value();
    }

private:
    std::string m_tag;
    std::optional<Error> m_error;
};
}  // namespace spider::tdl::parser

#endif  // SPIDER_TDL_PARSER_ERRORLISTENER_HPP
