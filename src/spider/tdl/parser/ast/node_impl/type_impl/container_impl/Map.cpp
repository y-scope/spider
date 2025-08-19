#include "Map.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/List.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Int.hpp>
#include <spider/tdl/parser/ast/SourceLocation.hpp>
#include <spider/tdl/parser/ast/utils.hpp>

using spider::tdl::parser::ast::node_impl::type_impl::container_impl::Map;
using MapErrorCodeCategory = ystdlib::error_handling::ErrorCategory<Map::ErrorCodeEnum>;

template <>
auto MapErrorCodeCategory::name() const noexcept -> char const* {
    return "spider::tdl::parser::ast::node_impl::type_impl::container_impl::Map";
}

template <>
auto MapErrorCodeCategory::message(Map::ErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case Map::ErrorCodeEnum::UnsupportedKeyType:
            return "Unsupported key type for Map.";
        default:
            return "Unknown error code enum";
    }
}

namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl {
namespace {
using spider::tdl::parser::ast::node_impl::type_impl::primitive_impl::Int;

/**
 * Given a key type, checks if it is supported as a key type for a Map.
 * @param key_type
 * @return Whether the key type is supported.
 */
[[nodiscard]] auto is_supported_key_type(Type const* key_type) -> bool;

auto is_supported_key_type(Type const* key_type) -> bool {
    if (nullptr != dynamic_cast<Int const*>(key_type)) {
        return true;
    }

    auto const* list_type{dynamic_cast<List const*>(key_type)};
    if (nullptr == list_type) {
        return false;
    }

    auto const* list_element_type{list_type->get_element_type()};
    if (auto const* int_type{dynamic_cast<Int const*>(list_element_type)}; nullptr != int_type) {
        if (int_type->get_spec() != IntSpec::Int8) {
            return false;
        }
        return true;
    }
    return false;
}
}  // namespace

auto Map::create(
        std::unique_ptr<Node> key_type,
        std::unique_ptr<Node> value_type,
        SourceLocation source_location
) -> ystdlib::error_handling::Result<std::unique_ptr<Node>> {
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Type>(key_type.get()));
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Type>(value_type.get()));

    // `key_type` has already been validated to be `Type` object.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    if (false == is_supported_key_type(static_cast<Type const*>(key_type.get()))) {
        return ErrorCode{ErrorCodeEnum::UnsupportedKeyType};
    }

    auto map{std::make_unique<Map>(Map{source_location})};
    YSTDLIB_ERROR_HANDLING_TRYV(map->add_child(std::move(key_type)));
    YSTDLIB_ERROR_HANDLING_TRYV(map->add_child(std::move(value_type)));
    return map;
}

auto Map::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    return fmt::format(
            "{}[Type[Container[Map]]]:\n{}KeyType:\n{}\n{}ValueType:\n{}",
            create_indentation(indentation_level),
            create_indentation(indentation_level + 1),
            YSTDLIB_ERROR_HANDLING_TRYX(get_key_type()->serialize_to_str(indentation_level + 2)),
            create_indentation(indentation_level + 1),
            YSTDLIB_ERROR_HANDLING_TRYX(get_value_type()->serialize_to_str(indentation_level + 2))
    );
}
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl
