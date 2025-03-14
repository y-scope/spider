#ifndef SPIDER_TESTS_STORAGETESTHELPER_HPP
#define SPIDER_TESTS_STORAGETESTHELPER_HPP
// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)

#include <concepts>
#include <memory>
#include <string>
#include <tuple>

#include "../../src/spider/storage/mysql/MySqlStorageFactory.hpp"
#include "../../src/spider/storage/StorageFactory.hpp"

namespace spider::test {
std::string const cMySqlStorageUrl
        = "jdbc:mariadb://localhost:3306/spider_test?user=root&password=password";

using StorageFactoryTypeList = std::tuple<core::MySqlStorageFactory>;

template <class T>
requires std::same_as<T, core::MySqlStorageFactory>
auto create_storage_factory() -> std::unique_ptr<core::StorageFactory> {
    return std::make_unique<T>(cMySqlStorageUrl);
}

template <class T>
requires std::same_as<T, core::MySqlStorageFactory>
auto get_storage_url() -> std::string {
    return cMySqlStorageUrl;
}

}  // namespace spider::test

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)
#endif  // SPIDER_TESTS_STORAGETESTHELPER_HPP
