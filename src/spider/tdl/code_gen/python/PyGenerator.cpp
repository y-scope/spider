#include "PyGenerator.hpp"

#include <cstddef>
#include <memory>
#include <ostream>
#include <tuple>
#include <utility>
#include <vector>

#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/Error.hpp>
#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/pass/analysis/StructSpecDependencyGraph.hpp>

namespace spider::tdl::code_gen::python {
namespace {
// The visitor pattern implementation uses recursion. Iterative implementation would take too long
// to implement. We will defer this to future PRs.
// NOLINTBEGIN(misc-no-recursion)

/**
 * Visitor for traversing the translation unit AST and generating Python code.
 */
class Visitor {
public:
    // Constructor
    explicit Visitor(
            std::shared_ptr<pass::analysis::StructSpecDependencyGraph> struct_spec_dependency_graph,
            std::ostream& out_stream
    )
            : m_struct_spec_dependency_graph{std::move(struct_spec_dependency_graph)},
              m_out_stream{&out_stream} {}

    // Delete copy & move constructor and copy assignment operator
    Visitor(Visitor const&) = delete;
    Visitor(Visitor&&) noexcept = delete;
    auto operator=(Visitor const&) -> Visitor& = delete;
    auto operator=(Visitor&&) -> Visitor& = delete;

    // Destructor
    ~Visitor() = default;

    // Methods
    /**
     * Visits a translation unit node and generates code for it.
     * @param tu The translation unit node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return An `Error` instance if:
     * - The struct specs have circular dependencies.
     * - The struct specs' dependency graph is invalid.
     * @return Forwards `visit_struct_spec`'s return values on failure.
     * @return Forwards `visit_child`'s return values on failure.
     */
    [[nodiscard]] auto visit_translation_unit(parser::ast::TranslationUnit const* tu)
            -> boost::outcome_v2::std_checked<void, Error>;

private:
    // Methods
    /**
     * Visits a named variable node and generates code for it.
     * @param named_var The named variable node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return Forwards `visit_named_var`'s return values on failure.
     */
    [[nodiscard]] auto visit_struct_spec(parser::ast::StructSpec const* struct_spec)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits a node by calling the appropriate visit function based on the node type.
     * @param node The node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return An `Error` instance if:
     * - The node is a nullptr.
     * - The node is a `TranslationUnit` or a `StructSpec` (these nodes should not show up as child
     *   nodes).
     * - The node's type is unrecognized.
     * @return Forwards `visit_namespace`'s return values on failure.
     * @return Forwards `visit_function`'s return values on failure.
     * @return Forwards `visit_named_var`'s return values on failure.
     * @return Forwards `visit_identifier`'s return values on failure.
     * @return Forwards `visit_type`'s return values on failure.
     */
    [[nodiscard]] auto visit_node(parser::ast::Node const* node)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits a namespace node and generates code for it.
     * @param ns The namespace node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return Forwards `visit_children`'s return values on failure.
     */
    [[nodiscard]] auto visit_namespace(parser::ast::Namespace const* ns)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits a function node and generates code for it.
     * @param func The function node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return Forwards `visit_named_var`'s return values on failure.
     * @return Forwards `visit_type`'s return values on failure.
     */
    [[nodiscard]] auto visit_function(parser::ast::Function const* func)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits a named variable node and generates code for it.
     * @param named_var The named variable node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return Forwards `visit_identifier`'s return values on failure.
     * @return Forwards `visit_type`'s return values on failure.
     */
    [[nodiscard]] auto visit_named_var(parser::ast::NamedVar const* named_var)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits an identifier node and generates code for it.
     * @param identifier The identifier node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success. This function always succeeds.
     */
    [[nodiscard]] auto visit_identifier(parser::ast::Identifier const* identifier)
            -> boost::outcome_v2::std_checked<void, Error> {
        *m_out_stream << identifier->get_name();
        return ystdlib::error_handling::success();
    }

    /**
     * Visits a type node and generates code for it.
     * @param type The type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return An `Error` instance if the type is unknown.
     * @return Forwards `visit_primitive_type`'s return values on failure.
     * @return Forwards `visit_struct_type`'s return values on failure.
     * @return Forwards `visit_list_type`'s return values on failure.
     * @return Forwards `visit_map_type`'s return values on failure.
     * @return Forwards `visit_tuple_type`'s return values on failure.
     */
    [[nodiscard]] auto visit_type(parser::ast::Type const* type)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits a primitive type node and generates code for it.
     * @param primitive_type The primitive type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return An `Error` instance if:
     * - The node is an `Int` but its spec is unsupported.
     * - The node is an `Float` but its spec is unsupported.
     * - The node's type is unrecognized.
     */
    [[nodiscard]] auto visit_primitive_type(parser::ast::Primitive const* primitive_type)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits a struct type node and generates code for it.
     * @param struct_type The struct type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success. This function always succeeds.
     */
    [[nodiscard]] auto visit_struct_type(parser::ast::Struct const* struct_type)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits a list type node and generates code for it.
     * @param list_type The list type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return Forwards `visit_type`'s return values on failure.
     */
    [[nodiscard]] auto visit_list_type(parser::ast::List const* list_type)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * visits a map type node and generates code for it.
     * @param map_type the map type node to visit.
     * @param out_stream the output stream to write the generated code.
     * @return A void result on success.
     * @return Forwards `visit_type`'s return values on failure.
     */
    [[nodiscard]] auto visit_map_type(parser::ast::Map const* map_type)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits a tuple type node and generates code for it.
     * @param tuple_type The tuple type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success.
     * @return Forwards `visit_type`'s return values on failure.
     */
    [[nodiscard]] auto visit_tuple_type(parser::ast::Tuple const* tuple_type)
            -> boost::outcome_v2::std_checked<void, Error>;

    /**
     * Visits all children of a node by calling `visit_node`.
     * @param node The node whose children to visit.
     * @param start_from The index of the child to start visiting from. Default is 0.
     * @return A void result on success.
     * @return Forwards `visit_node`'s return values on failure.
     */
    [[nodiscard]] auto visit_children(parser::ast::Node const* node, size_t start_from = 0)
            -> boost::outcome_v2::std_checked<void, Error> {
        for (size_t child_id{start_from}; child_id < node->get_num_children(); ++child_id) {
            YSTDLIB_ERROR_HANDLING_TRYV(visit_node(node->get_child(child_id).value()));
        }
        return boost::outcome_v2::success();
    }

    auto increase_indent() noexcept -> void { ++m_indent_level; }

    auto decrease_indent() noexcept -> void {
        if (m_indent_level > 0) {
            --m_indent_level;
        }
    }

    auto reset_indent() noexcept -> void { m_indent_level = 0; }

    auto generate_indentation() noexcept -> void {
        for (size_t i{0}; i < m_indent_level; ++i) {
            *m_out_stream << "    ";
        }
    }

    auto generate_newline() noexcept -> void { *m_out_stream << "\n"; }

    // Variables
    std::shared_ptr<pass::analysis::StructSpecDependencyGraph> m_struct_spec_dependency_graph;
    std::ostream* m_out_stream;
    size_t m_indent_level{0};
};

auto Visitor::visit_translation_unit(parser::ast::TranslationUnit const* tu)
        -> boost::outcome_v2::std_checked<void, Error> {
    reset_indent();
    *m_out_stream << "# Auto-generated Python code from TDL";
    generate_newline();
    generate_newline();

    *m_out_stream << "from dataclasses import dataclass";
    generate_newline();

    *m_out_stream << "import spider_py";
    generate_newline();

    generate_newline();
    generate_newline();

    auto const optional_topological_ordering{
            m_struct_spec_dependency_graph->get_struct_specs_in_topological_ordering()
    };
    if (false == optional_topological_ordering.has_value()) {
        return Error{
                "Cannot generate Python code for TDL files with cyclic struct definitions.",
                tu->get_source_location()
        };
    }
    for (auto const struct_spec_id : *optional_topological_ordering) {
        auto const struct_spec{
                m_struct_spec_dependency_graph->get_struct_spec_from_id(struct_spec_id)
        };
        if (nullptr == struct_spec) {
            return Error{
                    "Internal error: `StructSpec` ID does not map to a valid `StructSpec`.",
                    tu->get_source_location()
            };
        }
        YSTDLIB_ERROR_HANDLING_TRYV(visit_struct_spec(struct_spec.get()));
        generate_newline();
    }

    return visit_children(tu);
}

auto Visitor::visit_struct_spec(parser::ast::StructSpec const* struct_spec)
        -> boost::outcome_v2::std_checked<void, Error> {
    generate_indentation();
    *m_out_stream << "@dataclass";
    generate_newline();
    *m_out_stream << "class " << struct_spec->get_name() << ":";
    generate_newline();

    increase_indent();
    std::vector<parser::ast::NamedVar const*> fields;
    fields.reserve(struct_spec->get_num_fields());
    std::ignore = struct_spec->visit_fields(
            [&](parser::ast::NamedVar const& field) -> ystdlib::error_handling::Result<void> {
                fields.emplace_back(&field);
                return ystdlib::error_handling::success();
            }
    );
    for (auto const& field : fields) {
        generate_indentation();
        YSTDLIB_ERROR_HANDLING_TRYV(visit_named_var(field));
        generate_newline();
    }
    decrease_indent();

    generate_newline();
    return ystdlib::error_handling::success();
}

auto Visitor::visit_node(parser::ast::Node const* node)
        -> boost::outcome_v2::std_checked<void, Error> {
    if (nullptr == node) {
        return Error{"Internal error: NULL AST node encountered.", parser::SourceLocation{0, 0}};
    }

    if (auto const* tu{dynamic_cast<parser::ast::TranslationUnit const*>(node)}; nullptr != tu) {
        return Error{
                "Internal error: Unexpected `TranslationUnit` node.",
                tu->get_source_location()
        };
    }

    if (auto const* struct_spec{dynamic_cast<parser::ast::StructSpec const*>(node)};
        nullptr != struct_spec)
    {
        return Error{
                "Internal error: Unexpected `StructSpec` node.",
                struct_spec->get_source_location()
        };
    }

    if (auto const* ns{dynamic_cast<parser::ast::Namespace const*>(node)}; nullptr != ns) {
        return visit_namespace(ns);
    }

    if (auto const* func{dynamic_cast<parser::ast::Function const*>(node)}; nullptr != func) {
        return visit_function(func);
    }

    if (auto const* named_var{dynamic_cast<parser::ast::NamedVar const*>(node)};
        nullptr != named_var)
    {
        return visit_named_var(named_var);
    }

    if (auto const* identifier{dynamic_cast<parser::ast::Identifier const*>(node)};
        nullptr != identifier)
    {
        return visit_identifier(identifier);
    }

    if (auto const* type{dynamic_cast<parser::ast::Type const*>(node)}; nullptr != type) {
        return visit_type(type);
    }

    return Error{"Internal error: Unknown AST node type.", node->get_source_location()};
}

auto Visitor::visit_namespace(parser::ast::Namespace const* ns)
        -> boost::outcome_v2::std_checked<void, Error> {
    generate_indentation();
    *m_out_stream << "class " << ns->get_name() << ":";
    generate_newline();

    increase_indent();
    auto const&& result{visit_children(ns, 1)};
    decrease_indent();

    generate_newline();
    return result;
}

auto Visitor::visit_function(parser::ast::Function const* func)
        -> boost::outcome_v2::std_checked<void, Error> {
    generate_indentation();
    *m_out_stream << "@staticmethod";
    generate_newline();

    generate_indentation();
    *m_out_stream << "def " << func->get_name() << "(";

    // Params
    if (0 != func->get_num_params()) {
        generate_newline();
        increase_indent();
        std::vector<parser::ast::NamedVar const*> params;
        params.reserve(func->get_num_params());
        std::ignore = func->visit_params(
                [&](parser::ast::NamedVar const& param) -> ystdlib::error_handling::Result<void> {
                    params.emplace_back(&param);
                    return ystdlib::error_handling::success();
                }
        );
        for (auto const& param : params) {
            generate_indentation();
            YSTDLIB_ERROR_HANDLING_TRYV(visit_named_var(param));
            *m_out_stream << ",";
            generate_newline();
        }
        decrease_indent();
        generate_indentation();
    }

    // Return
    *m_out_stream << ")";
    if (func->has_return()) {
        *m_out_stream << " -> ";
        YSTDLIB_ERROR_HANDLING_TRYV(visit_type(func->get_return_type()));
    }
    *m_out_stream << ":";
    generate_newline();

    // Body
    increase_indent();
    generate_indentation();
    *m_out_stream << "pass";
    generate_newline();
    decrease_indent();

    generate_newline();
    return ystdlib::error_handling::success();
}

auto Visitor::visit_named_var(parser::ast::NamedVar const* named_var)
        -> boost::outcome_v2::std_checked<void, Error> {
    YSTDLIB_ERROR_HANDLING_TRYV(visit_identifier(named_var->get_id()));
    *m_out_stream << ": ";
    YSTDLIB_ERROR_HANDLING_TRYV(visit_type(named_var->get_type()));
    return ystdlib::error_handling::success();
}

auto Visitor::visit_type(parser::ast::Type const* type)
        -> boost::outcome_v2::std_checked<void, Error> {
    if (auto const* primitive{dynamic_cast<parser::ast::Primitive const*>(type)};
        nullptr != primitive)
    {
        return visit_primitive_type(primitive);
    }

    if (auto const* struct_type{dynamic_cast<parser::ast::Struct const*>(type)};
        nullptr != struct_type)
    {
        return visit_struct_type(struct_type);
    }

    if (auto const* list_type{dynamic_cast<parser::ast::List const*>(type)}; nullptr != list_type) {
        return visit_list_type(list_type);
    }

    if (auto const* map_type{dynamic_cast<parser::ast::Map const*>(type)}; nullptr != map_type) {
        return visit_map_type(map_type);
    }

    if (auto const* tuple_type{dynamic_cast<parser::ast::Tuple const*>(type)};
        nullptr != tuple_type)
    {
        return visit_tuple_type(tuple_type);
    }

    return Error{"Unknown `Type` node type.", type->get_source_location()};
}

auto Visitor::visit_primitive_type(parser::ast::Primitive const* primitive_type)
        -> boost::outcome_v2::std_checked<void, Error> {
    if (auto const* int_type{dynamic_cast<parser::ast::Int const*>(primitive_type)};
        nullptr != int_type)
    {
        switch (int_type->get_spec()) {
            case parser::ast::IntSpec::Int8:
                *m_out_stream << "spider_py.Int8";
                break;
            case parser::ast::IntSpec::Int16:
                *m_out_stream << "spider_py.Int16";
                break;
            case parser::ast::IntSpec::Int32:
                *m_out_stream << "spider_py.Int32";
                break;
            case parser::ast::IntSpec::Int64:
                *m_out_stream << "spider_py.Int64";
                break;
            default:
                return Error{"Unsupported integer type.", int_type->get_source_location()};
        }
        return boost::outcome_v2::success();
    }

    if (auto const* float_type{dynamic_cast<parser::ast::Float const*>(primitive_type)};
        nullptr != float_type)
    {
        switch (float_type->get_spec()) {
            case parser::ast::FloatSpec::Float:
                *m_out_stream << "spider_py.Float";
                break;
            case parser::ast::FloatSpec::Double:
                *m_out_stream << "spider_py.Double";
                break;
            default:
                return Error{"Unsupported float type.", float_type->get_source_location()};
        }
        return boost::outcome_v2::success();
    }

    if (auto const* bool_type{dynamic_cast<parser::ast::Bool const*>(primitive_type)};
        nullptr != bool_type)
    {
        *m_out_stream << "bool";
        return boost::outcome_v2::success();
    }

    return Error{"Unknown `Primitive` type.", primitive_type->get_source_location()};
}

auto Visitor::visit_struct_type(parser::ast::Struct const* struct_type)
        -> boost::outcome_v2::std_checked<void, Error> {
    *m_out_stream << struct_type->get_name();
    return boost::outcome_v2::success();
}

auto Visitor::visit_list_type(parser::ast::List const* list_type)
        -> boost::outcome_v2::std_checked<void, Error> {
    *m_out_stream << "list[";
    YSTDLIB_ERROR_HANDLING_TRYV(visit_type(list_type->get_element_type()));
    *m_out_stream << "]";
    return boost::outcome_v2::success();
}

auto Visitor::visit_map_type(parser::ast::Map const* map_type)
        -> boost::outcome_v2::std_checked<void, Error> {
    *m_out_stream << "dict[";
    YSTDLIB_ERROR_HANDLING_TRYV(visit_type(map_type->get_key_type()));
    *m_out_stream << ", ";
    YSTDLIB_ERROR_HANDLING_TRYV(visit_type(map_type->get_value_type()));
    *m_out_stream << "]";
    return boost::outcome_v2::success();
}

auto Visitor::visit_tuple_type(parser::ast::Tuple const* tuple_type)
        -> boost::outcome_v2::std_checked<void, Error> {
    *m_out_stream << "(";
    std::vector<parser::ast::Type const*> element_types;
    element_types.reserve(tuple_type->get_num_children());
    std::ignore = tuple_type->visit_children(
            [&](parser::ast::Node const& child) -> ystdlib::error_handling::Result<void> {
                // The factory function ensures that all children are of type `Type`.
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
                element_types.emplace_back(static_cast<parser::ast::Type const*>(&child));
                return ystdlib::error_handling::success();
            }
    );
    for (size_t i{0}; i < element_types.size(); ++i) {
        YSTDLIB_ERROR_HANDLING_TRYV(visit_type(element_types[i]));
        if (i + 1 < element_types.size()) {
            *m_out_stream << ", ";
        }
    }
    *m_out_stream << ")";
    return boost::outcome_v2::success();
}

// NOLINTEND(misc-no-recursion)
}  // namespace

auto PyGenerator::generate(std::ostream& out_stream)
        -> boost::outcome_v2::std_checked<void, Error> {
    return Visitor{get_struct_spec_dependency_graph(), out_stream}.visit_translation_unit(
            get_translation_unit()
    );
}
}  // namespace spider::tdl::code_gen::python
