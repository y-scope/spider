#ifndef SPIDER_TESTS_STORAGETESTHELPER_HPP
#define SPIDER_TESTS_STORAGETESTHELPER_HPP
// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)

#include "../../src/spider/storage/DataStorage.hpp"
#include "../../src/spider/storage/MetadataStorage.hpp"
#include "../../src/spider/storage/MysqlStorage.hpp"

#include <catch2/catch_test_macros.hpp>
#include <concepts>
#include <memory>
#include <tuple>
#include <utility>

namespace spider::test {
char const* const cStorageUrl
        = "jdbc:mariadb://localhost:3306/spider_test?user=root&password=password";

using DataStorageTypeList = std::tuple<core::MySqlDataStorage>;
using MetadataStorageTypeList = std::tuple<core::MySqlMetadataStorage>;
using StorageTypeList = std::tuple<std::tuple<core::MySqlMetadataStorage, core::MySqlDataStorage>>;

template <class T>
requires std::derived_from<T, core::DataStorage>
auto create_data_storage() -> std::unique_ptr<core::DataStorage> {
    std::unique_ptr<core::DataStorage> storage = std::make_unique<T>();
    REQUIRE(storage->connect(spider::test::cStorageUrl).success());
    REQUIRE(storage->initialize().success());
    return storage;
}

template <class T>
requires std::derived_from<T, core::MetadataStorage>
auto create_metadata_storage() -> std::unique_ptr<core::MetadataStorage> {
    std::unique_ptr<core::MetadataStorage> storage = std::make_unique<T>();
    REQUIRE(storage->connect(spider::test::cStorageUrl).success());
    REQUIRE(storage->initialize().success());
    return storage;
}

template <class M, class D>
requires std::derived_from<M, core::MetadataStorage> && std::derived_from<D, core::DataStorage>
auto create_storage(
) -> std::tuple<std::unique_ptr<core::MetadataStorage>, std::unique_ptr<core::DataStorage>> {
    std::unique_ptr<core::MetadataStorage> metadata_storage = std::make_unique<M>();
    REQUIRE(metadata_storage->connect(spider::test::cStorageUrl).success());
    REQUIRE(metadata_storage->initialize().success());
    std::unique_ptr<core::DataStorage> data_storage = std::make_unique<D>();
    REQUIRE(data_storage->connect(spider::test::cStorageUrl).success());
    REQUIRE(data_storage->initialize().success());
    return std::make_tuple(std::move(metadata_storage), std::move(data_storage));
}

}  // namespace spider::test

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)
#endif  // SPIDER_TESTS_STORAGETESTHELPER_HPP
