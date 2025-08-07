#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_MAP_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_MAP_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Container.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl {
class Map : public Container {
public:
    // Types
    enum class ErrorCodeEnum : uint8_t {
        UnsupportedKeyType = 1,
    };

    using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

    // Factory function
    /**
     * @param key_type
     * @param value_type
     * @return A result containing a unique pointer to a new `Map` instance with the given key and
     * value types on success, or an error code indicating the failure:
     * - Map::ErrorCodeEnum::UnsupportedKeyType if the `key_type` is not supported.
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto
    create(std::unique_ptr<Node> key_type, std::unique_ptr<Node> value_type)
            -> ystdlib::error_handling::Result<std::unique_ptr<Node>>;

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto get_key_type() const -> Type const* {
        // The factory function ensures that the first child is of type `Type`.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Type const*>(get_child_unsafe(0));
    }

    [[nodiscard]] auto get_value_type() const -> Type const* {
        // The factory function ensures that the first child is of type `Type`.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Type const*>(get_child_unsafe(1));
    }

private:
    // Constructor
    Map() = default;
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(
        spider::tdl::parser::ast::node_impl::type_impl::container_impl::Map::ErrorCodeEnum
);

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_MAP_HPP
