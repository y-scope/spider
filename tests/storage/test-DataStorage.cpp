#include "../../src/spider/storage/DataStorage.hpp"
#include "../../src/spider/storage/MysqlStorage.hpp"
#include "../../src/spider/core/Error.hpp"

#include <concepts>
#include <memory>

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>

namespace {

template <class T>
requires std::derived_from<T, spider::core::DataStorage>
auto create_data_storage() -> std::unique_ptr<spider::core::DataStorage> {
    return std::unique_ptr<spider::core::DataStorage>(static_cast<spider::core::DataStorage*>(new T()));
}

TEMPLATE_TEST_CASE("spider::core::DataStorage add and get task", "[storage]", spider::core::MySqlDataStorage) {
    std::unique_ptr<spider::core::DataStorage> storage = create_data_storage<TestType>();
    boost::uuids::random_generator gen;
    boost::uuids::uuid const id = gen();
    REQUIRE(spider::core::StorageErrType::Success == storage->connect("url", id).type);
}
}