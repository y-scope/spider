#include "PyGenerator.hpp"

#include <memory>
#include <ostream>

#include <boost/outcome/std_result.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/nodes.hpp>

namespace spider::tdl::code_gen::python {
namespace {
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

    /**
     * Visits a translation unit node and generates code for it.
     * @param tu The translation unit node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_translation_unit(parser::ast::TranslationUnit const* tu)
            -> ystdlib::error_handling::Result<void>;

private:
    // Methods
    /**
     * Visits a node by calling the appropriate visit function based on the node type.
     * @param node The node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_node(parser::ast::Node const* node)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a namespace node and generates code for it.
     * @param ns The namespace node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_namespace(parser::ast::Namespace const* ns)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a function node and generates code for it.
     * @param func The function node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_function(parser::ast::Function const* func)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a named variable node and generates code for it.
     * @param named_var The named variable node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_struct_spec(parser::ast::StructSpec const* struct_spec)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a named variable node and generates code for it.
     * @param named_var The named variable node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_named_var(parser::ast::NamedVar const* named_var)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits an identifier node and generates code for it.
     * @param identifier The identifier node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_identifier(parser::ast::Identifier const* identifier)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a type node and generates code for it.
     * @param type The type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_type(parser::ast::Type const* type)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a primitive type node and generates code for it.
     * @param primitive_type The primitive type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_primitive_type(parser::ast::Primitive const* primitive_type)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a struct type node and generates code for it.
     * @param struct_type The struct type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_struct_type(parser::ast::Struct const* struct_type)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a list type node and generates code for it.
     * @param list_type The list type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_list_type(parser::ast::List const* list_type)
            -> ystdlib::error_handling::Result<void>;

    /**
     * visits a map type node and generates code for it.
     * @param map_type the map type node to visit.
     * @param out_stream the output stream to write the generated code.
     * @return a void result on success, or an error code indicating the failure:
     * - todo
     */
    [[nodiscard]] auto visit_map_type(parser::ast::Map const* map_type)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Visits a tuple type node and generates code for it.
     * @param tuple_type The tuple type node to visit.
     * @param out_stream The output stream to write the generated code.
     * @return A void result on success, or an error code indicating the failure:
     * - TODO
     */
    [[nodiscard]] auto visit_tuple_type(parser::ast::Tuple const* tuple_type)
            -> ystdlib::error_handling::Result<void>;

    // Variables
    std::shared_ptr<pass::analysis::StructSpecDependencyGraph> m_struct_spec_dependency_graph;
    std::ostream* m_out_stream;
};
}  // namespace

auto PyGenerator::generate(std::ostream& out_stream)
        -> boost::outcome_v2::std_checked<void, Error> {
    return boost::outcome_v2::success();
}
}  // namespace spider::tdl::code_gen::python
