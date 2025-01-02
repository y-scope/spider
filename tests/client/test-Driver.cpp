// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <functional>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/client/Data.hpp"
#include "../../src/spider/client/Driver.hpp"
#include "../../src/spider/client/TaskContext.hpp"
#include "../../src/spider/client/TaskGraph.hpp"
#include "../storage/StorageTestHelper.hpp"

namespace {
TEST_CASE("Driver kv store", "[client][storage]") {
    spider::Driver driver{spider::test::cStorageUrl};
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

TEST_CASE("Driver data", "[client][storage]") {
    spider::Driver driver{spider::test::cStorageUrl};
    spider::Data<int> data = driver.get_data_builder<int>().build(5);
}

auto sum(spider::TaskContext&, int x, int y) -> int {
    return x + y;
}

SPIDER_REGISTER_TASK(sum);

TEST_CASE("Driver bind task", "[client][storage]") {
    spider::Driver driver{spider::test::cStorageUrl};

    spider::TaskGraph<int, int, int> graph_1 = driver.bind(&sum, &sum, 0);
    spider::Data<int> data = driver.get_data_builder<int>().build(1);
    spider::TaskGraph<int, int, int> graph_2 = driver.bind(&sum, &sum, data);
    spider::TaskGraph<int, int, int, int, int> graph_3 = driver.bind(&sum, &sum, &sum);
    spider::TaskGraph<int, int, int, int, int> graph_4 = driver.bind(&sum, graph_1, graph_2);
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
