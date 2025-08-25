grammar TaskDefLang;

@header {
#include <memory>
#include <utility>
#include <vector>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/parser/Exception.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>
}

translationUnit returns [std::unique_ptr<spider::tdl::parser::ast::TranslationUnit> tu]
@init {
    $tu = spider::tdl::parser::ast::TranslationUnit::create({
            $ctx->start->getLine(),
            $ctx->start->getCharPositionInLine()
    });
}
: (namespace {
    auto const ns_loc{$namespace.retval->get_source_location()};
    spider::tdl::parser::Exception::throw_tryv(
        $tu->add_namespace(std::move($namespace.retval)),
        ns_loc
    );
} | structDef {
    auto const struct_loc{$structDef.retval->get_source_location()};
    spider::tdl::parser::Exception::throw_tryv(
        $tu->add_struct_spec(std::move($structDef.retval)),
        struct_loc
    );
})* EOF
;

namespace returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: 'namespace' id '{' funcDefs '}' {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
    $retval = spider::tdl::parser::Exception::throw_tryx(
        spider::tdl::parser::ast::Namespace::create(
            std::move($id.retval),
            std::move($funcDefs.retval),
            loc
        ),
        loc
    );
}
;

funcDefs returns [std::vector<std::unique_ptr<spider::tdl::parser::ast::Node>> retval]
: funcDef {
    $retval.clear();
    $retval.emplace_back(std::move($funcDef.retval));
}
| parsed_funcs=funcDefs funcDef {
    $retval = std::move($parsed_funcs.retval);
    $retval.emplace_back(std::move($funcDef.retval));
}
;

funcDef returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: 'fn' id '(' params ')' ret ';' {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
    $retval = spider::tdl::parser::Exception::throw_tryx(
        spider::tdl::parser::ast::Function::create(
            std::move($id.retval),
            std::move($ret.retval),
            std::move($params.retval),
            loc
        ),
        loc
    );
}
;

ret returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: '->' retType {
    $retval = std::move($retType.retval);
}
| {
    $retval = nullptr;
}
;

params returns [std::vector<std::unique_ptr<spider::tdl::parser::ast::Node>> retval]
: namedVarList {
    $retval = std::move($namedVarList.retval);
}
| {
    $retval.clear();
}
;

namedVar returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: id ':' varType {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
    $retval = spider::tdl::parser::Exception::throw_tryx(
        spider::tdl::parser::ast::NamedVar::create(
            std::move($id.retval),
            std::move($varType.retval),
            loc
        ),
        loc
    );
}
;

namedVarList returns [std::vector<std::unique_ptr<spider::tdl::parser::ast::Node>> retval]
: namedVar {
    $retval.clear();
    $retval.emplace_back(std::move($namedVar.retval));
}
| parsed_named_vars=namedVarList ',' namedVar {
    $retval = std::move($parsed_named_vars.retval);
    $retval.emplace_back(std::move($namedVar.retval));
}
;

structDef returns [std::shared_ptr<spider::tdl::parser::ast::StructSpec> retval]
: 'struct' id '{' namedVarList (',')? '}' ';' {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
    $retval = spider::tdl::parser::Exception::throw_tryx(
        spider::tdl::parser::ast::StructSpec::create(
            std::move($id.retval),
            std::move($namedVarList.retval),
            loc
        ),
        loc
    );
}
;

id returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: ID {
    $retval = spider::tdl::parser::ast::Identifier::create(
        $ID.text,
        spider::tdl::parser::SourceLocation{
                $ctx->start->getLine(),
                $ctx->start->getCharPositionInLine()
        }
    );
}
;

varType returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: builtinType {
    $retval = std::move($builtinType.retval);
}
| id {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
    $retval = spider::tdl::parser::Exception::throw_tryx(
        spider::tdl::parser::ast::Struct::create(std::move($id.retval), loc),
        loc
    );
}
;

retType returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: varType {
    $retval = std::move($varType.retval);
}
| tupleType {
    $retval = std::move($tupleType.retval);
}
;

varTypeList returns [std::vector<std::unique_ptr<spider::tdl::parser::ast::Node>> retval]
: varType {
    $retval.clear();
    $retval.emplace_back(std::move($varType.retval));
}
| parsed_var_types=varTypeList ',' varType {
    $retval = std::move($parsed_var_types.retval);
    $retval.emplace_back(std::move($varType.retval));
}
|
;

listType returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: 'List' '<' varType '>' {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
    $retval = spider::tdl::parser::Exception::throw_tryx(
        spider::tdl::parser::ast::List::create(std::move($varType.retval), loc),
        loc
    );
}
;

mapType returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: 'Map' '<' key_type=varType ',' val_type=varType '>' {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
    $retval = spider::tdl::parser::Exception::throw_tryx(
        spider::tdl::parser::ast::Map::create(
                std::move($key_type.retval),
                std::move($val_type.retval),
                loc
        ),
        loc
    );
}
;

tupleType returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
: 'Tuple' '<' varTypeList '>' {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
    $retval = spider::tdl::parser::Exception::throw_tryx(
        spider::tdl::parser::ast::Tuple::create(std::move($varTypeList.retval), loc),
        loc
    );
}
;

builtinType returns [std::unique_ptr<spider::tdl::parser::ast::Node> retval]
@init {
    SourceLocation const loc{
        $ctx->start->getLine(),
        $ctx->start->getCharPositionInLine()
    };
}
: 'int8' {
    $retval = spider::tdl::parser::ast::Int::create(spider::tdl::parser::ast::IntSpec::Int8, loc);
}
| 'int16' {
    $retval = spider::tdl::parser::ast::Int::create(spider::tdl::parser::ast::IntSpec::Int16, loc);
}
| 'int32' {
    $retval = spider::tdl::parser::ast::Int::create(spider::tdl::parser::ast::IntSpec::Int32, loc);
}
| 'int64' {
    $retval = spider::tdl::parser::ast::Int::create(spider::tdl::parser::ast::IntSpec::Int64, loc);
}
| 'float' {
    $retval = spider::tdl::parser::ast::Float::create(
            spider::tdl::parser::ast::FloatSpec::Float,
            loc
    );
}
| 'double' {
    $retval = spider::tdl::parser::ast::Float::create(
            spider::tdl::parser::ast::FloatSpec::Double,
            loc
    );
}
| 'bool' {
    $retval = spider::tdl::parser::ast::Bool::create(loc);
}
| listType {
    $retval = std::move($listType.retval);
}
| mapType {
    $retval = std::move($mapType.retval);
}
;

ID: [a-zA-Z_][a-zA-Z0-9_]* ;
SPACE:  [ \t\r\n] -> skip ;
COMMENT: '//' (~[\r\n])* -> skip;
