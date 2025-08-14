// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Function.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/NamedVar.hpp>
#include <spider/tdl/parser/ast/node_impl/Namespace.hpp>
#include <spider/tdl/parser/ast/node_impl/StructSpec.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/List.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/Map.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/Tuple.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Bool.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Float.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Int.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Struct.hpp>

namespace {
/**
 * @param name
 * @return A struct AST node with the given name.
 */
[[nodiscard]] auto create_struct_node(std::string_view name)
        -> std::unique_ptr<spider::tdl::parser::ast::Node>;

/**
 * @param name
 * @param type
 * @return A named-var AST node with the given name and type.
 */
[[nodiscard]] auto
create_named_var(std::string_view name, std::unique_ptr<spider::tdl::parser::ast::Node> type)
        -> std::unique_ptr<spider::tdl::parser::ast::Node>;

/**
 * @param name
 * @return A function AST node with the given name which has zero parameter and returns an empty
 * tuple.
 */
[[nodiscard]] auto create_func(std::string_view name)
        -> std::unique_ptr<spider::tdl::parser::ast::Node>;

auto create_struct_node(std::string_view name) -> std::unique_ptr<spider::tdl::parser::ast::Node> {
    using spider::tdl::parser::ast::node_impl::Identifier;
    using spider::tdl::parser::ast::node_impl::type_impl::Struct;

    auto struct_node_result{Struct::create(Identifier::create(std::string{name}))};
    REQUIRE_FALSE(struct_node_result.has_error());
    return std::move(struct_node_result.value());
}

auto create_named_var(std::string_view name, std::unique_ptr<spider::tdl::parser::ast::Node> type)
        -> std::unique_ptr<spider::tdl::parser::ast::Node> {
    using spider::tdl::parser::ast::node_impl::Identifier;
    using spider::tdl::parser::ast::node_impl::NamedVar;
    using spider::tdl::parser::ast::node_impl::type_impl::Struct;

    auto named_var_result{NamedVar::create(Identifier::create(std::string{name}), std::move(type))};
    REQUIRE_FALSE(named_var_result.has_error());
    return std::move(named_var_result.value());
}

auto create_func(std::string_view name) -> std::unique_ptr<spider::tdl::parser::ast::Node> {
    using spider::tdl::parser::ast::node_impl::Function;
    using spider::tdl::parser::ast::node_impl::Identifier;
    using spider::tdl::parser::ast::node_impl::type_impl::container_impl::Tuple;

    auto tuple_result{Tuple::create({})};
    REQUIRE_FALSE(tuple_result.has_error());
    auto func_result{Function::create(
            Identifier::create(std::string{name}),
            std::move(tuple_result.value()),
            {}
    )};
    REQUIRE_FALSE(func_result.has_error());
    return std::move(func_result.value());
}

TEST_CASE("test-ast-node", "[tdl][ast][Node]") {
    using spider::tdl::parser::ast::FloatSpec;
    using spider::tdl::parser::ast::IntSpec;
    using spider::tdl::parser::ast::Node;
    using spider::tdl::parser::ast::node_impl::Function;
    using spider::tdl::parser::ast::node_impl::Identifier;
    using spider::tdl::parser::ast::node_impl::NamedVar;
    using spider::tdl::parser::ast::node_impl::Namespace;
    using spider::tdl::parser::ast::node_impl::StructSpec;
    using spider::tdl::parser::ast::node_impl::type_impl::container_impl::List;
    using spider::tdl::parser::ast::node_impl::type_impl::container_impl::Map;
    using spider::tdl::parser::ast::node_impl::type_impl::container_impl::Tuple;
    using spider::tdl::parser::ast::node_impl::type_impl::primitive_impl::Bool;
    using spider::tdl::parser::ast::node_impl::type_impl::primitive_impl::Float;
    using spider::tdl::parser::ast::node_impl::type_impl::primitive_impl::Int;
    using spider::tdl::parser::ast::node_impl::type_impl::Struct;
    using ystdlib::error_handling::Result;

    SECTION("Identifier") {
        constexpr std::string_view cTestName{"test_name"};
        constexpr std::string_view cSerializedIdentifier{"[Identifier]:test_name"};

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
        auto const [int_spec, expected_serialized_result] = GENERATE(
                std::make_pair(IntSpec::Int8, std::string_view{"[Type[Primitive[Int]]]:int8"}),
                std::make_pair(IntSpec::Int16, std::string_view{"[Type[Primitive[Int]]]:int16"}),
                std::make_pair(IntSpec::Int32, std::string_view{"[Type[Primitive[Int]]]:int32"}),
                std::make_pair(IntSpec::Int64, std::string_view{"[Type[Primitive[Int]]]:int64"})
        );

        auto const node{Int::create(int_spec)};
        auto const* int_node{dynamic_cast<Int const*>(node.get())};
        REQUIRE(nullptr != int_node);

        REQUIRE(int_node->get_spec() == int_spec);
        REQUIRE(int_node->get_num_children() == 0);

        auto const serialized_result{int_node->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        REQUIRE(serialized_result.value() == expected_serialized_result);
    }

    SECTION("Type Float") {
        auto const [float_spec, expected_serialized_result] = GENERATE(
                std::make_pair(
                        FloatSpec::Float,
                        std::string_view{"[Type[Primitive[Float]]]:float"}
                ),
                std::make_pair(
                        FloatSpec::Double,
                        std::string_view{"[Type[Primitive[Float]]]:double"}
                )
        );

        auto const node{Float::create(float_spec)};
        auto const* float_node{dynamic_cast<Float const*>(node.get())};
        REQUIRE(nullptr != float_node);

        REQUIRE(float_node->get_spec() == float_spec);
        REQUIRE(float_node->get_num_children() == 0);

        auto const serialized_result{float_node->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        REQUIRE(serialized_result.value() == expected_serialized_result);
    }

    SECTION("Type Bool") {
        auto const node{Bool::create()};
        auto const* bool_node{dynamic_cast<Bool const*>(node.get())};
        REQUIRE(nullptr != bool_node);

        REQUIRE(bool_node->get_num_children() == 0);

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
                "      KeyType:\n"
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
                "  KeyType:\n"
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

    SECTION("NamedVar") {
        auto id_result{Identifier::create("TestId")};
        auto map_result{Map::create(Int::create(IntSpec::Int64), Float::create(FloatSpec::Double))};
        REQUIRE_FALSE(map_result.has_error());
        auto named_var_result{
                NamedVar::create(std::move(id_result), std::move(map_result.value()))
        };
        REQUIRE_FALSE(named_var_result.has_error());
        auto const* named_var_node{dynamic_cast<NamedVar const*>(named_var_result.value().get())};
        REQUIRE(nullptr != named_var_node);

        REQUIRE(named_var_node->get_num_children() == 2);

        constexpr std::string_view cExpectedSerializedResult{
                "[NamedVar]:\n"
                "  Id:\n"
                "    [Identifier]:TestId\n"
                "  Type:\n"
                "    [Type[Container[Map]]]:\n"
                "      KeyType:\n"
                "        [Type[Primitive[Int]]]:int64\n"
                "      ValueType:\n"
                "        [Type[Primitive[Float]]]:double"
        };
        auto const serialized_result{named_var_node->serialize_to_str(0)};
        REQUIRE_FALSE(serialized_result.has_error());
        REQUIRE(serialized_result.value() == cExpectedSerializedResult);
    }

    SECTION("Tuple") {
        SECTION("Empty") {
            auto empty_tuple_result{Tuple::create({})};
            REQUIRE_FALSE(empty_tuple_result.has_error());
            auto const* tuple_node{dynamic_cast<Tuple const*>(empty_tuple_result.value().get())};
            REQUIRE(nullptr != tuple_node);

            REQUIRE(tuple_node->get_num_children() == 0);

            constexpr std::string_view cExpectedSerializedResult{"[Type[Container[Tuple]]]:Empty"};
            auto const serialized_result{tuple_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);
        }

        SECTION("Tuple with elements") {
            auto int_node{Int::create(IntSpec::Int64)};
            auto float_node{Float::create(FloatSpec::Double)};
            auto map_result{
                    Map::create(Int::create(IntSpec::Int64), Float::create(FloatSpec::Double))
            };
            REQUIRE_FALSE(map_result.has_error());
            std::vector<std::unique_ptr<Node>> elements;
            elements.emplace_back(std::move(int_node));
            elements.emplace_back(std::move(float_node));
            elements.emplace_back(std::move(map_result.value()));
            auto tuple_result{Tuple::create(std::move(elements))};
            REQUIRE_FALSE(tuple_result.has_error());
            auto const* tuple_node{dynamic_cast<Tuple const*>(tuple_result.value().get())};
            REQUIRE(nullptr != tuple_node);

            REQUIRE(tuple_node->get_num_children() == 3);

            constexpr std::string_view cExpectedSerializedResult{
                    "[Type[Container[Tuple]]]:\n"
                    "  Element[0]:\n"
                    "    [Type[Primitive[Int]]]:int64\n"
                    "  Element[1]:\n"
                    "    [Type[Primitive[Float]]]:double\n"
                    "  Element[2]:\n"
                    "    [Type[Container[Map]]]:\n"
                    "      KeyType:\n"
                    "        [Type[Primitive[Int]]]:int64\n"
                    "      ValueType:\n"
                    "        [Type[Primitive[Float]]]:double"
            };
            auto const serialized_result{tuple_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);
        }
    }

    SECTION("StructSpec") {
        constexpr std::string_view cTestStructName{"TestStruct"};

        auto int_field_result{
                NamedVar::create(Identifier::create("m_int"), Int::create(IntSpec::Int64))
        };
        REQUIRE_FALSE(int_field_result.has_error());
        auto float_field_result{
                NamedVar::create(Identifier::create("m_float"), Float::create(FloatSpec::Double))
        };
        REQUIRE_FALSE(float_field_result.has_error());
        auto map_result{Map::create(Int::create(IntSpec::Int64), Float::create(FloatSpec::Double))};
        REQUIRE_FALSE(map_result.has_error());
        auto map_field_result{
                NamedVar::create(Identifier::create("m_map"), std::move(map_result.value()))
        };
        REQUIRE_FALSE(map_field_result.has_error());
        std::vector<std::unique_ptr<Node>> fields;
        fields.emplace_back(std::move(int_field_result.value()));
        fields.emplace_back(std::move(float_field_result.value()));
        fields.emplace_back(std::move(map_field_result.value()));

        SECTION("Basic") {
            auto struct_spec_result{StructSpec::create(
                    Identifier::create(std::string{cTestStructName}),
                    std::move(fields)
            )};
            REQUIRE_FALSE(struct_spec_result.has_error());
            auto const* struct_spec_node{
                    dynamic_cast<StructSpec const*>(struct_spec_result.value().get())
            };
            REQUIRE(nullptr != struct_spec_node);

            REQUIRE(struct_spec_node->get_num_children() == 4);
            REQUIRE(struct_spec_node->get_name() == cTestStructName);

            constexpr std::string_view cExpectedSerializedResult{
                    "[StructSpec]:\n"
                    "  Name:TestStruct\n"
                    "  Fields[0]:\n"
                    "    [NamedVar]:\n"
                    "      Id:\n"
                    "        [Identifier]:m_int\n"
                    "      Type:\n"
                    "        [Type[Primitive[Int]]]:int64\n"
                    "  Fields[1]:\n"
                    "    [NamedVar]:\n"
                    "      Id:\n"
                    "        [Identifier]:m_float\n"
                    "      Type:\n"
                    "        [Type[Primitive[Float]]]:double\n"
                    "  Fields[2]:\n"
                    "    [NamedVar]:\n"
                    "      Id:\n"
                    "        [Identifier]:m_map\n"
                    "      Type:\n"
                    "        [Type[Container[Map]]]:\n"
                    "          KeyType:\n"
                    "            [Type[Primitive[Int]]]:int64\n"
                    "          ValueType:\n"
                    "            [Type[Primitive[Float]]]:double"
            };
            auto const serialized_result{struct_spec_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);
        }

        SECTION("Fields with duplicated name") {
            auto duplicated_int_field_result{
                    NamedVar::create(Identifier::create("m_int"), Int::create(IntSpec::Int64))
            };
            REQUIRE_FALSE(duplicated_int_field_result.has_error());
            // The execution model of `SECTION` ensures `fields` is not moved when this section is
            // executed, so using `fields` here is safe.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            fields.emplace_back(std::move(duplicated_int_field_result.value()));
            auto struct_spec_result{StructSpec::create(
                    Identifier::create(std::string{cTestStructName}),
                    std::move(fields)
            )};
            REQUIRE(struct_spec_result.has_error());
            REQUIRE(struct_spec_result.error()
                    == StructSpec::ErrorCode{StructSpec::ErrorCodeEnum::DuplicatedFieldName});
        }

        SECTION("Empty") {
            auto struct_spec_result{
                    StructSpec::create(Identifier::create(std::string{cTestStructName}), {})
            };
            REQUIRE(struct_spec_result.has_error());
            REQUIRE(struct_spec_result.error()
                    == StructSpec::ErrorCode{StructSpec::ErrorCodeEnum::EmptyStruct});
        }
    }

    SECTION("Struct") {
        constexpr std::string_view cTestStructName{"TestStruct"};

        // Create a `StructSpec`
        auto int_field_result{
                NamedVar::create(Identifier::create("m_int"), Int::create(IntSpec::Int64))
        };
        REQUIRE_FALSE(int_field_result.has_error());
        std::vector<std::unique_ptr<Node>> fields;
        fields.emplace_back(std::move(int_field_result.value()));

        auto struct_spec_result{StructSpec::create(
                Identifier::create(std::string{cTestStructName}),
                std::move(fields)
        )};
        REQUIRE_FALSE(struct_spec_result.has_error());
        REQUIRE(nullptr != dynamic_cast<StructSpec const*>(struct_spec_result.value().get()));

        SECTION("Struct with StructSpec") {
            auto struct_result{Struct::create(Identifier::create(std::string{cTestStructName}))};
            REQUIRE_FALSE(struct_result.has_error());
            auto* struct_node{dynamic_cast<Struct*>(struct_result.value().get())};
            REQUIRE(nullptr != struct_node);

            REQUIRE(struct_node->get_num_children() == 1);
            REQUIRE(cTestStructName == struct_node->get_name());
            REQUIRE(nullptr == struct_node->get_spec());

            constexpr std::string_view cExpectedSerializedResult{"[Type[Struct]]:\n"
                                                                 "  Name:\n"
                                                                 "    [Identifier]:TestStruct"};
            auto const serialized_result{struct_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);

            // Ensure nullptr can't be set as `StructSpec`
            auto const null_set_spec{struct_node->set_spec({})};
            REQUIRE(null_set_spec.has_error());
            REQUIRE(null_set_spec.error()
                    == Struct::ErrorCode{Struct::ErrorCodeEnum::NullStructSpec});
            REQUIRE(nullptr == struct_node->get_spec());

            // Set the `StructSpec` to the `Struct`
            REQUIRE_FALSE(struct_node->set_spec(struct_spec_result.value()).has_error());
            REQUIRE(nullptr != struct_node->get_spec());

            // Ensure `StructSpec` can't be set again
            auto const duplicated_set_spec{struct_node->set_spec(struct_spec_result.value())};
            REQUIRE(duplicated_set_spec.has_error());
            REQUIRE(duplicated_set_spec.error()
                    == Struct::ErrorCode{Struct::ErrorCodeEnum::StructSpecAlreadySet});
        }

        SECTION("Set spec to a wrong Struct") {
            auto struct_result{Struct::create(Identifier::create("WrongStruct"))};
            REQUIRE_FALSE(struct_result.has_error());
            auto* struct_node{dynamic_cast<Struct*>(struct_result.value().get())};
            REQUIRE(nullptr != struct_node);
            auto const set_spec_result{struct_node->set_spec(struct_spec_result.value())};
            REQUIRE(set_spec_result.has_error());
            REQUIRE(set_spec_result.error()
                    == Struct::ErrorCode{Struct::ErrorCodeEnum::StructSpecNameMismatch});
        }
    }

    SECTION("Function") {
        constexpr std::string_view cTestFuncName{"test_function"};
        constexpr std::string_view cTestStructName{"TestStruct"};

        auto function_name{Identifier::create(std::string{cTestFuncName})};

        std::vector<std::unique_ptr<Node>> tuple_elements;
        tuple_elements.emplace_back(Int::create(IntSpec::Int64));
        tuple_elements.emplace_back(create_struct_node(cTestStructName));
        tuple_elements.emplace_back(Bool::create());

        auto return_tuple_result{Tuple::create(std::move(tuple_elements))};
        REQUIRE_FALSE(return_tuple_result.has_error());

        std::vector<std::unique_ptr<Node>> parameters;
        parameters.emplace_back(create_named_var("param_0", Int::create(IntSpec::Int64)));
        parameters.emplace_back(create_named_var("param_1", create_struct_node(cTestStructName)));

        SECTION("Basic") {
            auto func_result{Function::create(
                    std::move(function_name),
                    std::move(return_tuple_result.value()),
                    std::move(parameters)
            )};
            REQUIRE_FALSE(func_result.has_error());
            auto const* func_node{dynamic_cast<Function const*>(func_result.value().get())};
            REQUIRE(nullptr != func_node);

            REQUIRE(func_node->get_num_children() == 4);
            REQUIRE(func_node->get_num_params() == 2);
            REQUIRE(func_node->get_name() == cTestFuncName);
            REQUIRE(nullptr != func_node->get_return_type());

            constexpr std::string_view cExpectedSerializedResult{
                    "[Function]:\n"
                    "  Name:test_function\n"
                    "  Return:\n"
                    "    [Type[Container[Tuple]]]:\n"
                    "      Element[0]:\n"
                    "        [Type[Primitive[Int]]]:int64\n"
                    "      Element[1]:\n"
                    "        [Type[Struct]]:\n"
                    "          Name:\n"
                    "            [Identifier]:TestStruct\n"
                    "      Element[2]:\n"
                    "        [Type[Primitive[Bool]]]\n"
                    "  Params[0]:\n"
                    "    [NamedVar]:\n"
                    "      Id:\n"
                    "        [Identifier]:param_0\n"
                    "      Type:\n"
                    "        [Type[Primitive[Int]]]:int64\n"
                    "  Params[1]:\n"
                    "    [NamedVar]:\n"
                    "      Id:\n"
                    "        [Identifier]:param_1\n"
                    "      Type:\n"
                    "        [Type[Struct]]:\n"
                    "          Name:\n"
                    "            [Identifier]:TestStruct"
            };
            auto const serialized_result{func_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);
        }

        SECTION("No return type") {
            // The execution model of `SECTION` ensures objects are not moved when this section is
            // executed, so use move below is safe.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            auto func_result{Function::create(std::move(function_name), {}, std::move(parameters))};
            REQUIRE_FALSE(func_result.has_error());
            auto const* func_node{dynamic_cast<Function const*>(func_result.value().get())};
            REQUIRE(nullptr != func_node);

            REQUIRE(func_node->get_num_children() == 3);
            REQUIRE(func_node->get_num_params() == 2);
            REQUIRE(func_node->get_name() == cTestFuncName);
            REQUIRE(nullptr == func_node->get_return_type());

            constexpr std::string_view cExpectedSerializedResult{
                    "[Function]:\n"
                    "  Name:test_function\n"
                    "  Return:\n"
                    "    void\n"
                    "  Params[0]:\n"
                    "    [NamedVar]:\n"
                    "      Id:\n"
                    "        [Identifier]:param_0\n"
                    "      Type:\n"
                    "        [Type[Primitive[Int]]]:int64\n"
                    "  Params[1]:\n"
                    "    [NamedVar]:\n"
                    "      Id:\n"
                    "        [Identifier]:param_1\n"
                    "      Type:\n"
                    "        [Type[Struct]]:\n"
                    "          Name:\n"
                    "            [Identifier]:TestStruct"
            };
            auto const serialized_result{func_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);
        }

        SECTION("Empty param list") {
            // The execution model of `SECTION` ensures objects are not moved when this section is
            // executed, so use move below is safe.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            auto func_result{Function::create(
                    std::move(function_name),
                    std::move(return_tuple_result.value()),
                    {}
            )};
            REQUIRE_FALSE(func_result.has_error());
            auto const* func_node{dynamic_cast<Function const*>(func_result.value().get())};
            REQUIRE(nullptr != func_node);

            REQUIRE(func_node->get_num_children() == 2);
            REQUIRE(func_node->get_num_params() == 0);
            REQUIRE(func_node->get_name() == cTestFuncName);
            REQUIRE(nullptr != func_node->get_return_type());

            constexpr std::string_view cExpectedSerializedResult{
                    "[Function]:\n"
                    "  Name:test_function\n"
                    "  Return:\n"
                    "    [Type[Container[Tuple]]]:\n"
                    "      Element[0]:\n"
                    "        [Type[Primitive[Int]]]:int64\n"
                    "      Element[1]:\n"
                    "        [Type[Struct]]:\n"
                    "          Name:\n"
                    "            [Identifier]:TestStruct\n"
                    "      Element[2]:\n"
                    "        [Type[Primitive[Bool]]]\n"
                    "  No Params"
            };
            auto const serialized_result{func_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);
        }

        SECTION("Empty param list and no return") {
            // The execution model of `SECTION` ensures objects are not moved when this section is
            // executed, so use move below is safe.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            auto func_result{Function::create(std::move(function_name), {}, {})};
            REQUIRE_FALSE(func_result.has_error());
            auto const* func_node{dynamic_cast<Function const*>(func_result.value().get())};
            REQUIRE(nullptr != func_node);

            REQUIRE(func_node->get_num_children() == 1);
            REQUIRE(func_node->get_num_params() == 0);
            REQUIRE(func_node->get_name() == cTestFuncName);
            REQUIRE(nullptr == func_node->get_return_type());

            constexpr std::string_view cExpectedSerializedResult{"[Function]:\n"
                                                                 "  Name:test_function\n"
                                                                 "  Return:\n"
                                                                 "    void\n"
                                                                 "  No Params"};
            auto const serialized_result{func_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);
        }

        SECTION("Duplicated param names") {
            // The execution model of `SECTION` ensures objects are not moved when this section is
            // executed, so use and move these objects should be safe.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            parameters.emplace_back(create_named_var("param_0", Int::create(IntSpec::Int64)));
            auto func_result{Function::create(std::move(function_name), {}, std::move(parameters))};
            REQUIRE(func_result.has_error());
            REQUIRE(func_result.error()
                    == Function::ErrorCode{Function::ErrorCodeEnum::DuplicatedParamName});
        }
    }

    SECTION("Namespace") {
        constexpr std::string_view cTestNamespaceName{"TestNamespace"};

        std::vector<std::unique_ptr<Node>> functions;
        functions.emplace_back(create_func("func_0"));
        functions.emplace_back(create_func("func_1"));

        SECTION("Basic") {
            auto namespace_result{Namespace::create(
                    Identifier::create(std::string{cTestNamespaceName}),
                    std::move(functions)
            )};
            REQUIRE_FALSE(namespace_result.has_error());
            auto const* namespace_node{
                    dynamic_cast<Namespace const*>(namespace_result.value().get())
            };
            REQUIRE(nullptr != namespace_node);

            REQUIRE(namespace_node->get_name() == cTestNamespaceName);
            REQUIRE(namespace_node->get_num_children() == 3);

            constexpr std::string_view cExpectedSerializedResult{
                    "[Namespace]:\n"
                    "  Name:TestNamespace\n"
                    "  Func[0]:\n"
                    "    [Function]:\n"
                    "      Name:func_0\n"
                    "      Return:\n"
                    "        [Type[Container[Tuple]]]:Empty\n"
                    "      No Params\n"
                    "  Func[1]:\n"
                    "    [Function]:\n"
                    "      Name:func_1\n"
                    "      Return:\n"
                    "        [Type[Container[Tuple]]]:Empty\n"
                    "      No Params"
            };
            auto const serialized_result{namespace_node->serialize_to_str(0)};
            REQUIRE_FALSE(serialized_result.has_error());
            REQUIRE(serialized_result.value() == cExpectedSerializedResult);
        }

        SECTION("Empty") {
            auto namespace_result{
                    Namespace::create(Identifier::create(std::string{cTestNamespaceName}), {})
            };
            REQUIRE(namespace_result.has_error());
            REQUIRE(namespace_result.error()
                    == Namespace::ErrorCode{Namespace::ErrorCodeEnum::EmptyNamespace});
        }

        SECTION("Duplicated names") {
            // The execution model of `SECTION` ensures objects are not moved when this section is
            // executed, so use and move `functions` is safe.
            // NOLINTBEGIN(bugprone-use-after-move)
            functions.emplace_back(create_func("func_0"));
            auto namespace_result{Namespace::create(
                    Identifier::create(std::string{cTestNamespaceName}),
                    std::move(functions)
            )};
            // NOLINTEND(bugprone-use-after-move)
            REQUIRE(namespace_result.has_error());
            REQUIRE(namespace_result.error()
                    == Namespace::ErrorCode{Namespace::ErrorCodeEnum::DuplicatedFunctionName});
        }
    }
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
