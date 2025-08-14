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
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/NamedVar.hpp>
#include <spider/tdl/parser/ast/node_impl/StructSpec.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/List.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/Map.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/Tuple.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Bool.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Float.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Int.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Struct.hpp>

namespace {
TEST_CASE("test-ast-node", "[tdl][ast][Node]") {
    using spider::tdl::parser::ast::FloatSpec;
    using spider::tdl::parser::ast::IntSpec;
    using spider::tdl::parser::ast::Node;
    using spider::tdl::parser::ast::node_impl::Identifier;
    using spider::tdl::parser::ast::node_impl::NamedVar;
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

        SECTION("With Struct") {
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

            SECTION("StructSpec serialization") {
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

            auto struct_result{Struct::create(Identifier::create(std::string{cTestStructName}))};
            REQUIRE_FALSE(struct_result.has_error());
            auto* struct_node{dynamic_cast<Struct*>(struct_result.value().get())};
            REQUIRE(nullptr != struct_node);

            REQUIRE(struct_node->get_num_children() == 1);
            REQUIRE(cTestStructName == struct_node->get_name());

            REQUIRE_FALSE(struct_node->set_spec(struct_spec_result.value()).has_error());
            auto const duplicated_set_spec{struct_node->set_spec(struct_spec_result.value())};
            REQUIRE(duplicated_set_spec.has_error());
            REQUIRE(duplicated_set_spec.error()
                    == Struct::ErrorCode{Struct::ErrorCodeEnum::StructSpecAlreadySet});

            SECTION("Struct serialization") {
                constexpr std::string_view cExpectedSerializedResult{"[Type[Struct]]:\n"
                                                                     "  Name:\n"
                                                                     "    [Identifier]:TestStruct"};
                auto const serialized_result{struct_node->serialize_to_str(0)};
                REQUIRE_FALSE(serialized_result.has_error());
                REQUIRE(serialized_result.value() == cExpectedSerializedResult);
            }

            SECTION("Set spec to a wrong Struct") {
                auto wrong_struct_result{Struct::create(Identifier::create("WrongStruct"))};
                REQUIRE_FALSE(wrong_struct_result.has_error());
                auto* wrong_struct_node{dynamic_cast<Struct*>(wrong_struct_result.value().get())};
                REQUIRE(nullptr != wrong_struct_node);
                auto const set_spec_result{wrong_struct_node->set_spec(struct_spec_result.value())};
                REQUIRE(set_spec_result.has_error());
                REQUIRE(set_spec_result.error()
                        == Struct::ErrorCode{Struct::ErrorCodeEnum::StructSpecNameMismatch});
            }
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
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
