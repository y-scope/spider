#include "Function.hpp"

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
#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/SourceLocation.hpp>
#include <spider/tdl/parser/ast/utils.hpp>

using spider::tdl::parser::ast::node_impl::Function;
using FunctionErrorCodeCategory = ystdlib::error_handling::ErrorCategory<Function::ErrorCodeEnum>;

template <>
auto FunctionErrorCodeCategory::name() const noexcept -> char const* {
    return "spider::tdl::parser::ast::node_impl::Function";
}

template <>
auto FunctionErrorCodeCategory::message(Function::ErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case Function::ErrorCodeEnum::DuplicatedParamName:
            return "The parameters have duplicated names.";
        default:
            return "Unknown error code enum";
    }
}

namespace spider::tdl::parser::ast::node_impl {
auto Function::create(
        std::unique_ptr<Node> name,
        std::unique_ptr<Node> return_type,
        std::vector<std::unique_ptr<Node>> params,
        SourceLocation source_location
) -> ystdlib::error_handling::Result<std::unique_ptr<Node>> {
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Identifier>(name.get()));

    bool const has_return{nullptr != return_type};
    if (has_return) {
        YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Type>(return_type.get()));
    }

    absl::flat_hash_set<std::string_view> param_names;
    for (auto const& param : params) {
        YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<NamedVar>(param.get()));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        auto const param_name{static_cast<NamedVar const&>(*param).get_id()->get_name()};
        if (param_names.contains(param_name)) {
            return ErrorCode{ErrorCodeEnum::DuplicatedParamName};
        }
        param_names.emplace(param_name);
    }

    auto function{std::make_unique<Function>(Function{has_return, source_location})};
    YSTDLIB_ERROR_HANDLING_TRYV(function->add_child(std::move(name)));
    if (has_return) {
        YSTDLIB_ERROR_HANDLING_TRYV(function->add_child(std::move(return_type)));
    }
    for (auto& param : params) {
        YSTDLIB_ERROR_HANDLING_TRYV(function->add_child(std::move(param)));
    }
    return function;
}

auto Function::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    std::vector<std::string> serialized_params;
    YSTDLIB_ERROR_HANDLING_TRYV(
            visit_params([&](NamedVar const& param) -> ystdlib::error_handling::Result<void> {
                serialized_params.emplace_back(
                        fmt::format(
                                "{}Params[{}]:\n{}",
                                create_indentation(indentation_level + 1),
                                serialized_params.size(),
                                YSTDLIB_ERROR_HANDLING_TRYX(
                                        param.serialize_to_str(indentation_level + 2)
                                )
                        )
                );
                return ystdlib::error_handling::success();
            })
    );

    std::string const serialized_return_type{
            has_return() ? YSTDLIB_ERROR_HANDLING_TRYX(
                                   get_return_type()->serialize_to_str(indentation_level + 2)
                           )
                         : fmt::format("{}void", create_indentation(indentation_level + 2))
    };

    if (false == serialized_params.empty()) {
        return fmt::format(
                "{}[Function]:\n{}Name:{}\n{}Return:\n{}\n{}",
                create_indentation(indentation_level),
                create_indentation(indentation_level + 1),
                get_name(),
                create_indentation(indentation_level + 1),
                serialized_return_type,
                fmt::join(serialized_params, "\n")
        );
    }

    return fmt::format(
            "{}[Function]:\n{}Name:{}\n{}Return:\n{}\n{}No Params",
            create_indentation(indentation_level),
            create_indentation(indentation_level + 1),
            get_name(),
            create_indentation(indentation_level + 1),
            serialized_return_type,
            create_indentation(indentation_level + 1)
    );
}
}  // namespace spider::tdl::parser::ast::node_impl
