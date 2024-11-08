// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)

#include "../../src/spider/storage/MysqlStorage.hpp"
#include "StorageTestHelper.hpp"

#include <boost/uuid/uuid_io.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <chrono>
#include <memory>
#include <ranges>
#include <thread>

namespace {

TEMPLATE_TEST_CASE("Driver heartbeat", "[storage]", spider::core::MySqlMetadataStorage) {
    std::unique_ptr<spider::core::MetadataStorage> storage
            = spider::test::create_metadata_storage<TestType>();

    constexpr double cDuration = 100;

    // Add driver should succeed
    boost::uuids::random_generator gen;
    boost::uuids::uuid const driver_id = gen();
    REQUIRE(storage->add_driver(driver_id, "127.0.0.1").success());

    std::string addr;
    REQUIRE(storage->get_driver(driver_id, &addr).success());
    REQUIRE("127.0.0.1" == addr);

    std::vector<boost::uuids::uuid> ids{};
    // Driver should not time out
    REQUIRE(storage->heartbeat_timeout(cDuration, &ids).success());
    // Because other tests may run in parallel, just check `ids` don't have `driver_id`
    REQUIRE(std::ranges::none_of(ids, [driver_id](boost::uuids::uuid id) {
        return id == driver_id;
    }));
    ids.clear();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Driver should time out
    REQUIRE(storage->heartbeat_timeout(cDuration, &ids).success());
    REQUIRE(!ids.empty());
    REQUIRE(std::ranges::any_of(ids, [driver_id](boost::uuids::uuid id) { return id == driver_id; })
    );
    ids.clear();

    // Update heartbeat
    REQUIRE(storage->update_heartbeat(driver_id).success());
    // Driver should not time out
    REQUIRE(storage->heartbeat_timeout(cDuration, &ids).success());
    REQUIRE(std::ranges::none_of(ids, [driver_id](boost::uuids::uuid id) {
        return id == driver_id;
    }));
}

}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity)
