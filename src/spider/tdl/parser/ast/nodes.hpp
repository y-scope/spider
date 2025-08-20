/**
 * A super header that imports all AST nodes and hides the implementation namespaces.
 */
#ifndef SPIDER_TDL_PARSER_AST_NODES_HPP
#define SPIDER_TDL_PARSER_AST_NODES_HPP

#include <spider/tdl/parser/ast/node_impl/Function.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/NamedVar.hpp>
#include <spider/tdl/parser/ast/node_impl/Namespace.hpp>
#include <spider/tdl/parser/ast/node_impl/StructSpec.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Container.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/List.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/Map.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/container_impl/Tuple.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Primitive.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Bool.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Float.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/primitive_impl/Int.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Struct.hpp>

// IWYU pragma: begin_exports
#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/Node.hpp>

// IWYU pragma: end_exports

namespace spider::tdl::parser::ast {
using Function = node_impl::Function;
using Identifier = node_impl::Identifier;
using NamedVar = node_impl::NamedVar;
using Namespace = node_impl::Namespace;
using StructSpec = node_impl::StructSpec;
using Type = node_impl::Type;
using Container = node_impl::type_impl::Container;
using Primitive = node_impl::type_impl::Primitive;
using Struct = node_impl::type_impl::Struct;
using List = node_impl::type_impl::container_impl::List;
using Map = node_impl::type_impl::container_impl::Map;
using Tuple = node_impl::type_impl::container_impl::Tuple;
using Int = node_impl::type_impl::primitive_impl::Int;
using Float = node_impl::type_impl::primitive_impl::Float;
using Bool = node_impl::type_impl::primitive_impl::Bool;
}  // namespace spider::tdl::parser::ast

#endif  // SPIDER_TDL_PARSER_AST_NODES_HPP
