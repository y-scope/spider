#include "Tuple.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/utils.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl {
auto Tuple::create(std::vector<std::unique_ptr<Node>> elements, SourceLocation source_location)
        -> ystdlib::error_handling::Result<std::unique_ptr<Node>> {
    for (auto const& type : elements) {
        YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Type>(type.get()));
    }

    auto tuple{std::make_unique<Tuple>(Tuple{source_location})};
    for (auto& type : elements) {
        YSTDLIB_ERROR_HANDLING_TRYV(tuple->add_child(std::move(type)));
    }
    return tuple;
}

auto Tuple::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    constexpr std::string_view cTypeTag{"[Type[Container[Tuple]]]"};

    if (is_empty()) {
        return fmt::format("{}{}:Empty", create_indentation(indentation_level), cTypeTag);
    }

    std::vector<std::string> serialized_children;
    YSTDLIB_ERROR_HANDLING_TRYV(
            visit_children([&](Node const& child) -> ystdlib::error_handling::Result<void> {
                serialized_children.emplace_back(
                        fmt::format(
                                "{}Element[{}]:\n{}",
                                create_indentation(indentation_level + 1),
                                serialized_children.size(),
                                YSTDLIB_ERROR_HANDLING_TRYX(
                                        child.serialize_to_str(indentation_level + 2)
                                )
                        )
                );
                return ystdlib::error_handling::success();
            })
    );
    return fmt::format(
            "{}{}:\n{}",
            create_indentation(indentation_level),
            cTypeTag,
            fmt::join(serialized_children, "\n")
    );
}
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl
