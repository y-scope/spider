set(SPIDER_TDL_TEST_SOURCES
    test-parser.cpp
    test-parser-ast.cpp
    test-pass-analysis-DetectUndefinedStruct.cpp
    test-pass-analysis-StructSpecDependencyGraph.cpp
)

add_executable(unitTest)
target_sources(unitTest PRIVATE ${SPIDER_TDL_TEST_SOURCES})
target_link_libraries(
    unitTest
    PRIVATE
        Catch2::Catch2WithMain
        spider::tdl
)
