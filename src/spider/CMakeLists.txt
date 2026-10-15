set(SPIDER_TDL_ANTLR_GENERATED_SOURCES
    tdl/parser/antlr_generated/TaskDefLangBaseVisitor.cpp
    tdl/parser/antlr_generated/TaskDefLangBaseVisitor.h
    tdl/parser/antlr_generated/TaskDefLangLexer.cpp
    tdl/parser/antlr_generated/TaskDefLangLexer.h
    tdl/parser/antlr_generated/TaskDefLangParser.cpp
    tdl/parser/antlr_generated/TaskDefLangParser.h
    tdl/parser/antlr_generated/TaskDefLangVisitor.cpp
    tdl/parser/antlr_generated/TaskDefLangVisitor.h
    CACHE INTERNAL
    "spider task definition language ANTLR generated source files"
)

set(SPIDER_TDL_SHARED_SOURCES
    tdl/parser/ast/Node.cpp
    tdl/parser/ast/node_impl/Function.cpp
    tdl/parser/ast/node_impl/Identifier.cpp
    tdl/parser/ast/node_impl/NamedVar.cpp
    tdl/parser/ast/node_impl/Namespace.cpp
    tdl/parser/ast/node_impl/StructSpec.cpp
    tdl/parser/ast/node_impl/TranslationUnit.cpp
    tdl/parser/ast/node_impl/type_impl/container_impl/List.cpp
    tdl/parser/ast/node_impl/type_impl/container_impl/Map.cpp
    tdl/parser/ast/node_impl/type_impl/container_impl/Tuple.cpp
    tdl/parser/ast/node_impl/type_impl/primitive_impl/Bool.cpp
    tdl/parser/ast/node_impl/type_impl/primitive_impl/Float.cpp
    tdl/parser/ast/node_impl/type_impl/primitive_impl/Int.cpp
    tdl/parser/ast/node_impl/type_impl/Struct.cpp
    tdl/parser/ast/utils.cpp
    tdl/parser/parse.cpp
    tdl/pass/analysis/DetectUndefinedStruct.cpp
    tdl/pass/analysis/StructSpecDependencyGraph.cpp
    CACHE INTERNAL
    "spider task definition language shared source files"
)

set(SPIDER_TDL_SHARED_HEADERS
    tdl/Error.hpp
    tdl/parser/ast/Node.hpp
    tdl/parser/ast/FloatSpec.hpp
    tdl/parser/ast/IntSpec.hpp
    tdl/parser/ast/node_impl/Function.hpp
    tdl/parser/ast/node_impl/Identifier.hpp
    tdl/parser/ast/node_impl/NamedVar.hpp
    tdl/parser/ast/node_impl/Namespace.hpp
    tdl/parser/ast/node_impl/StructSpec.hpp
    tdl/parser/ast/node_impl/TranslationUnit.hpp
    tdl/parser/ast/node_impl/Type.hpp
    tdl/parser/ast/node_impl/type_impl/Container.hpp
    tdl/parser/ast/node_impl/type_impl/container_impl/List.hpp
    tdl/parser/ast/node_impl/type_impl/container_impl/Map.hpp
    tdl/parser/ast/node_impl/type_impl/container_impl/Tuple.hpp
    tdl/parser/ast/node_impl/type_impl/Primitive.hpp
    tdl/parser/ast/node_impl/type_impl/primitive_impl/Bool.hpp
    tdl/parser/ast/node_impl/type_impl/primitive_impl/Float.hpp
    tdl/parser/ast/node_impl/type_impl/primitive_impl/Int.hpp
    tdl/parser/ast/node_impl/type_impl/Struct.hpp
    tdl/parser/ast/nodes.hpp
    tdl/parser/ast/utils.hpp
    tdl/parser/ErrorListener.hpp
    tdl/parser/Exception.hpp
    tdl/parser/parse.hpp
    tdl/parser/SourceLocation.hpp
    tdl/pass/analysis/DetectUndefinedStruct.hpp
    tdl/pass/analysis/StructSpecDependencyGraph.hpp
    tdl/pass/Pass.hpp
    tdl/pass/utils.hpp
    CACHE INTERNAL
    "spider task definition language shared header files"
)

add_library(spider_tdl)
target_sources(
    spider_tdl
    PRIVATE
        ${SPIDER_TDL_ANTLR_GENERATED_SOURCES}
        ${SPIDER_TDL_SHARED_SOURCES}
)
target_sources(spider_tdl PUBLIC ${SPIDER_TDL_SHARED_HEADERS})
target_include_directories(spider_tdl PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(
    spider_tdl
    PUBLIC
        absl::flat_hash_set
        antlr4_static
        fmt::fmt
        ystdlib::error_handling
)

add_library(spider::tdl ALIAS spider_tdl)
