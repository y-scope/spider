#include "StructSpec.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/NamedVar.hpp>
#include <spider/tdl/parser/ast/utils.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>

using spider::tdl::parser::ast::node_impl::StructSpec;
using StructSpecErrorCodeCategory
        = ystdlib::error_handling::ErrorCategory<StructSpec::ErrorCodeEnum>;

template <>
auto StructSpecErrorCodeCategory::name() const noexcept -> char const* {
    return "spider::tdl::parser::ast::node_impl::StructSpec";
}

template <>
auto StructSpecErrorCodeCategory::message(StructSpec::ErrorCodeEnum error_enum) const
        -> std::string {
    switch (error_enum) {
        case StructSpec::ErrorCodeEnum::DuplicatedFieldName:
            return "The struct spec has duplicated field names.";
        case StructSpec::ErrorCodeEnum::EmptyStruct:
            return "The struct spec is empty.";
        default:
            return "Unknown error code enum";
    }
}

namespace spider::tdl::parser::ast::node_impl {
auto StructSpec::create(
        std::unique_ptr<Node> name,
        std::vector<std::unique_ptr<Node>> fields,
        SourceLocation source_location
) -> ystdlib::error_handling::Result<std::shared_ptr<StructSpec>> {
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Identifier>(name.get()));

    if (fields.empty()) {
        return ErrorCode{ErrorCodeEnum::EmptyStruct};
    }

    absl::flat_hash_set<std::string_view> field_names;
    for (auto const& field : fields) {
        YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<NamedVar>(field.get()));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        auto const field_name{static_cast<NamedVar const&>(*field).get_id()->get_name()};
        if (field_names.contains(field_name)) {
            return ErrorCode{ErrorCodeEnum::DuplicatedFieldName};
        }
        field_names.emplace(field_name);
    }

    auto struct_spec{std::make_shared<StructSpec>(StructSpec{source_location})};
    YSTDLIB_ERROR_HANDLING_TRYV(struct_spec->add_child(std::move(name)));
    for (auto& field : fields) {
        YSTDLIB_ERROR_HANDLING_TRYV(struct_spec->add_child(std::move(field)));
    }
    return struct_spec;
}

auto StructSpec::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    std::vector<std::string> serialized_fields;
    YSTDLIB_ERROR_HANDLING_TRYV(
            visit_fields([&](NamedVar const& child) -> ystdlib::error_handling::Result<void> {
                serialized_fields.emplace_back(
                        fmt::format(
                                "{}Fields[{}]:\n{}",
                                create_indentation(indentation_level + 1),
                                serialized_fields.size(),
                                YSTDLIB_ERROR_HANDLING_TRYX(
                                        child.serialize_to_str(indentation_level + 2)
                                )
                        )
                );
                return ystdlib::error_handling::success();
            })
    );
    return fmt::format(
            "{}[StructSpec]{}:\n{}Name:\n{}\n{}",
            create_indentation(indentation_level),
            get_source_location().serialize_to_str(),
            create_indentation(indentation_level + 1),
            YSTDLIB_ERROR_HANDLING_TRYX(
                    get_child_unsafe(0)->serialize_to_str(indentation_level + 2)
            ),
            fmt::join(serialized_fields, "\n")
    );
}
}  // namespace spider::tdl::parser::ast::node_impl
