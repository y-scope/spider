// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <optional>
#include <string>

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/client/Data.hpp"
#include "../../src/spider/client/Driver.hpp"
#include "../../src/spider/client/TaskContext.hpp"
#include "../../src/spider/client/TaskGraph.hpp"
#include "../storage/StorageTestHelper.hpp"

namespace {
TEMPLATE_LIST_TEST_CASE(
        "Driver kv store",
        "[client][storage]",
        spider::test::StorageFactoryTypeList
) {
    std::string const storage_url = spider::test::get_storage_url<TestType>();
    spider::Driver driver{storage_url};
    driver.kv_store_insert("key", "value");

    // Get value by key should succeed
    std::optional<std::string> const result = driver.kv_store_get("key");
    REQUIRE(result.has_value());
    if (result.has_value()) {
        REQUIRE(result.value() == "value");
    }
    // Get value by wrong key should fail
    std::optional<std::string> const fail_result = driver.kv_store_get("wrong_key");
    REQUIRE(!fail_result.has_value());
}

TEMPLATE_LIST_TEST_CASE("Driver data", "[client][storage]", spider::test::StorageFactoryTypeList) {
    std::string const storage_url = spider::test::get_storage_url<TestType>();
    spider::Driver driver{storage_url};
    spider::Data<int> const data = driver.get_data_builder<int>().build(1);
}

auto sum(spider::TaskContext&, int x, int y) -> int {
    return x + y;
}

auto test_driver(spider::TaskContext&, spider::Data<int>& x) -> int {
    return x.get();
}

SPIDER_REGISTER_TASK(sum);
SPIDER_REGISTER_TASK(test_driver);

TEMPLATE_LIST_TEST_CASE(
        "Driver bind task",
        "[client][storage]",
        spider::test::StorageFactoryTypeList
) {
    std::string const storage_url = spider::test::get_storage_url<TestType>();
    spider::Driver driver{storage_url};

    spider::TaskGraph<int, int, int> const graph_1 = driver.bind(&sum, &sum, 0);
    spider::TaskGraph<int, int, int, int, int> const graph_3 = driver.bind(&sum, &sum, &sum);
    spider::TaskGraph<int, int, int, int, int> const graph_4 = driver.bind(&sum, graph_1, graph_1);
}

TEMPLATE_LIST_TEST_CASE(
        "Driver bind task with data",
        "[client][storage]",
        spider::test::StorageFactoryTypeList
) {
    std::string const storage_url = spider::test::get_storage_url<TestType>();
    spider::Driver driver{storage_url};

    spider::Data<int> data = driver.get_data_builder<int>().build(1);
    spider::TaskGraph<int> const graph_1 = driver.bind(&test_driver, data);
    spider::TaskGraph<int, int, int> const graph_2 = driver.bind(&sum, &sum, graph_1);
}

}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
