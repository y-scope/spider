// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)
#include "../../src/spider/storage/DataStorage.hpp"
#include "../../src/spider/storage/MysqlStorage.hpp"
#include "../../src/spider/core/Error.hpp"
#include "../../src/spider/core/Data.hpp"

#include <concepts>
#include <memory>

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "StorageTestHelper.hpp"
#include "../utils/CoreDataUtils.hpp"

namespace {
template <class T>
requires std::derived_from<T, spider::core::DataStorage>
auto create_data_storage() -> std::unique_ptr<spider::core::DataStorage> {
    return std::unique_ptr<spider::core::DataStorage>(static_cast<spider::core::DataStorage*>(new T()));
}


TEMPLATE_TEST_CASE("Add, get and remove data", "[storage]", spider::core::MySqlDataStorage) {
    std::unique_ptr<spider::core::DataStorage> storage = create_data_storage<TestType>();
    REQUIRE(storage->connect(spider::test::cStorageUrl).success());
    REQUIRE(storage->initialize().success());

    // Add data
    spider::core::Data const data{"value"};
    REQUIRE(storage->add_data(data).success());

    // Add data with same id again should fail
    spider::core::Data const data_same_id{data.get_id(), "value2"};
    REQUIRE(spider::core::StorageErrType::DuplicateKeyErr == storage->add_data(data_same_id).type);

    // Get data should match
    spider::core::Data result{"temp"};
    REQUIRE(storage->get_data(data.get_id(), &result).success());
    REQUIRE(spider::core::data_equal(data, result));

    // Remove data should succeed
    REQUIRE(storage->remove_data(data.get_id()).success());

    // Get data should fail
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr == storage->get_data(data.get_id(), &result).type);
}
}
// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)
