#include "Namespace.hpp"

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
#include <spider/tdl/parser/ast/node_impl/Function.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/Namespace.hpp>
#include <spider/tdl/parser/ast/utils.hpp>

using spider::tdl::parser::ast::node_impl::Namespace;
using NamespaceErrorCodeCategory = ystdlib::error_handling::ErrorCategory<Namespace::ErrorCodeEnum>;

template <>
auto NamespaceErrorCodeCategory::name() const noexcept -> char const* {
    return "spider::tdl::parser::ast::node_impl::Namespace";
}

template <>
auto NamespaceErrorCodeCategory::message(Namespace::ErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case Namespace::ErrorCodeEnum::DuplicatedFunctionName:
            return "The namespace has duplicated function names.";
        case Namespace::ErrorCodeEnum::EmptyNamespace:
            return "The namespace is empty.";
        default:
            return "Unknown error code enum";
    }
}

namespace spider::tdl::parser::ast::node_impl {
auto Namespace::create(std::unique_ptr<Node> name, std::vector<std::unique_ptr<Node>> functions)
        -> ystdlib::error_handling::Result<std::unique_ptr<Node>> {
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Identifier>(name.get()));

    if (functions.empty()) {
        return ErrorCode{ErrorCodeEnum::EmptyNamespace};
    }

    absl::flat_hash_set<std::string_view> fun_names;
    for (auto const& func : functions) {
        YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Function>(func.get()));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        auto const func_name{static_cast<Function const&>(*func).get_name()};
        if (fun_names.contains(func_name)) {
            return ErrorCode{ErrorCodeEnum::DuplicatedFunctionName};
        }
        fun_names.emplace(func_name);
    }

    auto function{std::make_unique<Namespace>(Namespace{})};
    YSTDLIB_ERROR_HANDLING_TRYV(function->add_child(std::move(name)));
    for (auto& func : functions) {
        YSTDLIB_ERROR_HANDLING_TRYV(function->add_child(std::move(func)));
    }
    return function;
}

auto Namespace::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    std::vector<std::string> serialized_funcs;
    YSTDLIB_ERROR_HANDLING_TRYV(
            visit_functions([&](Function const& child) -> ystdlib::error_handling::Result<void> {
                serialized_funcs.emplace_back(
                        fmt::format(
                                "{}Func[{}]:\n{}",
                                create_indentation(indentation_level + 1),
                                serialized_funcs.size(),
                                YSTDLIB_ERROR_HANDLING_TRYX(
                                        child.serialize_to_str(indentation_level + 2)
                                )
                        )
                );
                return ystdlib::error_handling::success();
            })
    );

    return fmt::format(
            "{}[Namespace]:\n{}Name:{}\n{}",
            create_indentation(indentation_level),
            create_indentation(indentation_level + 1),
            get_name(),
            fmt::join(serialized_funcs, "\n")
    );
}
}  // namespace spider::tdl::parser::ast::node_impl
