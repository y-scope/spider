#ifndef SPIDER_TESTS_STORAGETESTHELPER_HPP
#define SPIDER_TESTS_STORAGETESTHELPER_HPP
// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)

#include <concepts>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>

#include <boost/process/v2/environment.hpp>

#include <spider/storage/mysql/MySqlStorageFactory.hpp>
#include <spider/storage/StorageFactory.hpp>

namespace spider::test {
constexpr std::string_view cMySqlStorageUrl
        = "jdbc:mariadb://localhost:3306/spider_test?user=root&password=password";

using StorageFactoryTypeList = std::tuple<core::MySqlStorageFactory>;

template <class T>
requires std::same_as<T, core::MySqlStorageFactory>
auto get_storage_url() -> std::string {
    auto const env = boost::process::v2::environment::current();
    for (auto const& entry : env) {
        if ("SPIDER_STORAGE_URL" == entry.key().string()) {
            return entry.value().string();
        }
    }
    return std::string{cMySqlStorageUrl};
}

template <class T>
requires std::same_as<T, core::MySqlStorageFactory>
auto create_storage_factory() -> std::unique_ptr<core::StorageFactory> {
    return std::make_unique<T>(get_storage_url<T>());
}
}  // namespace spider::test

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)
#endif  // SPIDER_TESTS_STORAGETESTHELPER_HPP
