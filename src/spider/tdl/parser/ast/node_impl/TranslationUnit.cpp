#include "TranslationUnit.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Namespace.hpp>
#include <spider/tdl/parser/ast/node_impl/StructSpec.hpp>
#include <spider/tdl/parser/ast/utils.hpp>

using spider::tdl::parser::ast::node_impl::TranslationUnit;
using TranslationUnitErrorCodeCategory
        = ystdlib::error_handling::ErrorCategory<TranslationUnit::ErrorCodeEnum>;

template <>
auto TranslationUnitErrorCodeCategory::name() const noexcept -> char const* {
    return "spider::tdl::parser::ast::node_impl::TranslationUnit";
}

template <>
auto TranslationUnitErrorCodeCategory::message(TranslationUnit::ErrorCodeEnum error_enum) const
        -> std::string {
    switch (error_enum) {
        case TranslationUnit::ErrorCodeEnum::DuplicatedNamespaceName:
            return "The translation unit contains duplicated namespace names.";
        case TranslationUnit::ErrorCodeEnum::DuplicatedStructSpecName:
            return "The translation unit contains duplicated struct spec names.";
        default:
            return "Unknown error code enum";
    }
}

namespace spider::tdl::parser::ast::node_impl {
auto TranslationUnit::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    std::vector<std::string_view> struct_spec_names;
    struct_spec_names.reserve(m_struct_spec_table.size());
    for (auto const& [name, _] : m_struct_spec_table) {
        struct_spec_names.emplace_back(name);
    }
    std::ranges::sort(struct_spec_names);

    std::vector<std::string> serialized_struct_specs;
    serialized_struct_specs.reserve(struct_spec_names.size());
    for (auto const name : struct_spec_names) {
        serialized_struct_specs.emplace_back(YSTDLIB_ERROR_HANDLING_TRYX(
                m_struct_spec_table.at(name)->serialize_to_str(indentation_level + 2)
        ));
    }

    std::vector<std::string> serialized_namespaces;
    serialized_namespaces.reserve(get_num_children());
    YSTDLIB_ERROR_HANDLING_TRYV(
            visit_children([&](Node const& child) -> ystdlib::error_handling::Result<void> {
                YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Namespace>(&child));
                serialized_namespaces.emplace_back(
                        YSTDLIB_ERROR_HANDLING_TRYX(child.serialize_to_str(indentation_level + 2))
                );
                return ystdlib::error_handling::success();
            })
    );

    return fmt::format(
            "{}[TranslationUnit]{}:\n{}StructSpecs:\n{}\n{}Namespaces:\n{}",
            create_indentation(indentation_level),
            get_source_location().serialize_to_str(),
            create_indentation(indentation_level + 1),
            fmt::join(serialized_struct_specs, "\n"),
            create_indentation(indentation_level + 1),
            fmt::join(serialized_namespaces, "\n")
    );
}

auto TranslationUnit::add_namespace(std::unique_ptr<Node> namespace_node)
        -> ystdlib::error_handling::Result<void> {
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Namespace>(namespace_node.get()));
    // The previous check ensures the node is of type `Namespace`.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    auto const* ns{static_cast<Namespace const*>(namespace_node.get())};
    YSTDLIB_ERROR_HANDLING_TRYV(
            visit_children([&](Node const& child) -> ystdlib::error_handling::Result<void> {
                YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Namespace>(&child));
                // The previous check ensures the node is of type `Namespace`.
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
                auto const& existing_ns{static_cast<Namespace const&>(child)};
                if (existing_ns.get_name() == ns->get_name()) {
                    return ErrorCode{ErrorCodeEnum::DuplicatedNamespaceName};
                }
                return ystdlib::error_handling::success();
            })
    );
    return add_child(std::move(namespace_node));
}

auto TranslationUnit::add_struct_spec(std::shared_ptr<StructSpec const> const& struct_spec)
        -> ystdlib::error_handling::Result<void> {
    auto const name{struct_spec->get_name()};
    if (m_struct_spec_table.contains(name)) {
        return ErrorCode{ErrorCodeEnum::DuplicatedStructSpecName};
    }
    m_struct_spec_table.emplace(name, struct_spec);
    return ystdlib::error_handling::success();
}
}  // namespace spider::tdl::parser::ast::node_impl
