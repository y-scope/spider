#include "Struct.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/StructSpec.hpp>
#include <spider/tdl/parser/ast/SourceLocation.hpp>
#include <spider/tdl/parser/ast/utils.hpp>

using spider::tdl::parser::ast::node_impl::type_impl::Struct;
using StructErrorCodeCategory = ystdlib::error_handling::ErrorCategory<Struct::ErrorCodeEnum>;

template <>
auto StructErrorCodeCategory::name() const noexcept -> char const* {
    return "spider::tdl::parser::ast::node_impl::type_impl::Struct";
}

template <>
auto StructErrorCodeCategory::message(Struct::ErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case Struct::ErrorCodeEnum::NullStructSpec:
            return "The struct spec is NULL.";
        case Struct::ErrorCodeEnum::StructSpecAlreadySet:
            return "The struct spec is already set.";
        case Struct::ErrorCodeEnum::StructSpecNameMismatch:
            return "The struct spec name does not match the type name.";
        default:
            return "Unknown error code enum";
    }
}

namespace spider::tdl::parser::ast::node_impl::type_impl {
auto Struct::create(std::unique_ptr<Node> name, SourceLocation source_location)
        -> ystdlib::error_handling::Result<std::unique_ptr<Node>> {
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Identifier>(name.get()));

    auto struct_node{std::make_unique<Struct>(Struct{source_location})};
    YSTDLIB_ERROR_HANDLING_TRYV(struct_node->add_child(std::move(name)));
    return struct_node;
}

auto Struct::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    return fmt::format(
            "{}[Type[Struct]]:\n{}Name:\n{}",
            create_indentation(indentation_level),
            create_indentation(indentation_level + 1),
            YSTDLIB_ERROR_HANDLING_TRYX(
                    // The factory function ensures that the first child is of type `Identifier`.
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
                    get_child_unsafe(0)->serialize_to_str(indentation_level + 2)
            )
    );
}

auto Struct::set_spec(std::shared_ptr<StructSpec> spec) -> ystdlib::error_handling::Result<void> {
    if (nullptr != m_spec) {
        return Struct::ErrorCode{Struct::ErrorCodeEnum::StructSpecAlreadySet};
    }

    if (nullptr == spec) {
        return Struct::ErrorCode{Struct::ErrorCodeEnum::NullStructSpec};
    }

    if (get_name() != spec->get_name()) {
        return Struct::ErrorCode{Struct::ErrorCodeEnum::StructSpecNameMismatch};
    }

    m_spec = std::move(spec);
    return ystdlib::error_handling::success();
}
}  // namespace spider::tdl::parser::ast::node_impl::type_impl
