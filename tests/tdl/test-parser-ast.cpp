// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <string>
#include <string_view>
#include <utility>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/List.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/Map.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Bool.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Float.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Int.hpp>
#include <spider/tdl/parser/ast/utils.hpp>

namespace {
TEST_CASE("test-ast-node", "[tdl][ast][Node]") {
    using spider::tdl::parser::ast::FloatSpec;
    using spider::tdl::parser::ast::IntSpec;
    using spider::tdl::parser::ast::Node;
    using spider::tdl::parser::ast::node_impl::Identifier;
    using spider::tdl::parser::ast::node_impl::type_impl::container_impl::List;
    using spider::tdl::parser::ast::node_impl::type_impl::container_impl::Map;
    using spider::tdl::parser::ast::node_impl::type_impl::primitive_impl::Bool;
    using spider::tdl::parser::ast::node_impl::type_impl::primitive_impl::Float;
    using spider::tdl::parser::ast::node_impl::type_impl::primitive_impl::Int;
    using spider::tdl::parser::ast::serialize_float_spec;
    using spider::tdl::parser::ast::serialize_int_spec;
    using ystdlib::error_handling::Result;

    SECTION("Identifier") {
        constexpr std::string_view cTestName{"test_name"};
        constexpr std::string_view cSerializedIdentifier{"[Identifier]: test_name"};

        auto const node{Identifier::create(std::string{cTestName})};
        auto const* identifier{dynamic_cast<Identifier const*>(node.get())};
        REQUIRE(nullptr != identifier);

        REQUIRE(nullptr == identifier->get_parent());
        REQUIRE(identifier->get_name() == cTestName);

        auto const serialized_result{identifier->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        REQUIRE(serialized_result.value() == cSerializedIdentifier);
    }

    SECTION("Type Int") {
        auto const int_spec
                = GENERATE(IntSpec::Int8, IntSpec::Int16, IntSpec::Int32, IntSpec::Int64);
        auto const serialized_int_spec_result{serialize_int_spec(int_spec)};
        REQUIRE_FALSE(serialized_int_spec_result.has_error());

        auto const node{Int::create(int_spec)};
        auto const* int_node{dynamic_cast<Int const*>(node.get())};
        REQUIRE(nullptr != int_node);

        REQUIRE(int_node->get_spec() == int_spec);

        constexpr std::string_view cExpectedSerializedResultPrefix{"[Type[Primitive[Int]]]:"};
        auto const serialized_result{int_node->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        auto const expected_serialized_result{
                std::string{cExpectedSerializedResultPrefix}
                + std::string{serialized_int_spec_result.value()}
        };
        REQUIRE(serialized_result.value() == expected_serialized_result);
    }

    SECTION("Type Float") {
        auto const float_spec = GENERATE(FloatSpec::Float, FloatSpec::Double);
        auto const serialized_float_spec_result{serialize_float_spec(float_spec)};
        REQUIRE_FALSE(serialized_float_spec_result.has_error());

        auto const node{Float::create(float_spec)};
        auto const* float_node{dynamic_cast<Float const*>(node.get())};
        REQUIRE(nullptr != float_node);

        REQUIRE(float_node->get_spec() == float_spec);

        constexpr std::string_view cExpectedSerializedResultPrefix{"[Type[Primitive[Float]]]:"};
        auto const serialized_result{float_node->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        auto const expected_serialized_result{
                std::string{cExpectedSerializedResultPrefix}
                + std::string{serialized_float_spec_result.value()}
        };
        REQUIRE(serialized_result.value() == expected_serialized_result);
    }

    SECTION("Type Bool") {
        auto const node{Bool::create()};
        auto const* bool_node{dynamic_cast<Bool const*>(node.get())};
        REQUIRE(nullptr != bool_node);

        constexpr std::string_view cExpectedSerializedResult{"[Type[Primitive[Bool]]]"};
        auto const serialized_result{bool_node->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        REQUIRE(serialized_result.value() == cExpectedSerializedResult);
    }

    SECTION("List of Map") {
        auto map_result{Map::create(Int::create(IntSpec::Int64), Float::create(FloatSpec::Double))};
        REQUIRE_FALSE(map_result.has_error());
        auto list_result{List::create(std::move(map_result.value()))};
        REQUIRE_FALSE(list_result.has_error());
        auto const* list_node{dynamic_cast<List const*>(list_result.value().get())};
        REQUIRE(nullptr != list_node);

        REQUIRE(list_node->get_num_children() == 1);

        constexpr std::string_view cExpectedSerializedResult{
                "[Type[Container[List]]]:\n"
                "  ElementType:\n"
                "    [Type[Container[Map]]]:\n"
                "      KeyTpe:\n"
                "        [Type[Primitive[Int]]]:int64\n"
                "      ValueType:\n"
                "        [Type[Primitive[Float]]]:double"
        };
        auto const serialized_result{list_node->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        REQUIRE(serialized_result.value() == cExpectedSerializedResult);
    }

    SECTION("Map of List") {
        auto key_list_result{List::create(Int::create(IntSpec::Int8))};
        REQUIRE_FALSE(key_list_result.has_error());
        auto value_list_result{List::create(Float::create(FloatSpec::Float))};
        REQUIRE_FALSE(value_list_result.has_error());
        auto map_result{Map::create(
                std::move(key_list_result.value()),
                std::move(value_list_result.value())
        )};
        REQUIRE_FALSE(map_result.has_error());
        auto const* map_node{dynamic_cast<Map const*>(map_result.value().get())};
        REQUIRE(nullptr != map_node);

        REQUIRE(map_node->get_num_children() == 2);

        constexpr std::string_view cExpectedSerializedResult{
                "[Type[Container[Map]]]:\n"
                "  KeyTpe:\n"
                "    [Type[Container[List]]]:\n"
                "      ElementType:\n"
                "        [Type[Primitive[Int]]]:int8\n"
                "  ValueType:\n"
                "    [Type[Container[List]]]:\n"
                "      ElementType:\n"
                "        [Type[Primitive[Float]]]:float"
        };
        auto const serialized_result{map_node->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        REQUIRE(serialized_result.value() == cExpectedSerializedResult);
    }

    SECTION("Invalid inputs for container type creation") {
        constexpr std::string_view cTestName{"test_name"};
        auto list_result{List::create(Identifier::create(std::string{cTestName}))};
        REQUIRE(list_result.has_error());
        REQUIRE(list_result.error()
                == Node::ErrorCode{Node::ErrorCodeEnum::UnexpectedChildNodeType});

        auto invalid_key_type_map_result{
                Map::create(Identifier::create(std::string{cTestName}), Int::create(IntSpec::Int64))
        };
        REQUIRE(invalid_key_type_map_result.has_error());
        REQUIRE(invalid_key_type_map_result.error()
                == Node::ErrorCode{Node::ErrorCodeEnum::UnexpectedChildNodeType});

        auto invalid_value_type_map_result{
                Map::create(Int::create(IntSpec::Int64), Identifier::create(std::string{cTestName}))
        };
        REQUIRE(invalid_value_type_map_result.has_error());
        REQUIRE(invalid_value_type_map_result.error()
                == Node::ErrorCode{Node::ErrorCodeEnum::UnexpectedChildNodeType});
    }

    SECTION("Unsupported key types in Map") {
        // We can't enum all types. Just asserting two types to ensure that the error is propagated
        // correctly.
        auto unsupported_primitive_key_type_map_result{
                Map::create(Float::create(FloatSpec::Float), Int::create(IntSpec::Int64))
        };
        REQUIRE(unsupported_primitive_key_type_map_result.has_error());
        REQUIRE(unsupported_primitive_key_type_map_result.error()
                == Map::ErrorCode{Map::ErrorCodeEnum::UnsupportedKeyType});

        auto list_result{List::create(Int::create(IntSpec::Int64))};
        REQUIRE_FALSE(list_result.has_error());
        auto unsupported_list_key_type_map_result{
                Map::create(std::move(list_result.value()), Int::create(IntSpec::Int64))
        };
        REQUIRE(unsupported_list_key_type_map_result.has_error());
        REQUIRE(unsupported_list_key_type_map_result.error()
                == Map::ErrorCode{Map::ErrorCodeEnum::UnsupportedKeyType});
    }
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
