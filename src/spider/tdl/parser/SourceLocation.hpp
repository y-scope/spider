#ifndef SPIDER_TDL_PARSER_SOURCELOCATION_HPP
#define SPIDER_TDL_PARSER_SOURCELOCATION_HPP

#include <cstddef>

namespace spider::tdl::parser {
class SourceLocation {
public:
    // Constructor
    SourceLocation(size_t line, size_t column) : m_line{line}, m_column{column} {}

    // Methods
    [[nodiscard]] auto get_line() const noexcept -> size_t { return m_line; }

    [[nodiscard]] auto get_column() const noexcept -> size_t { return m_column; }

private:
    // Variables
    size_t m_line;
    size_t m_column;
};
}  // namespace spider::tdl::parser

#endif  // SPIDER_TDL_PARSER_SOURCELOCATION_HPP
