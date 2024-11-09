// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include "../../src/spider/core/Error.hpp"
#include "../../src/spider/storage/MetadataStorage.hpp"
#include "StorageTestHelper.hpp"

#include <algorithm>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace {

TEMPLATE_LIST_TEST_CASE("Driver heartbeat", "[storage]", spider::test::MetadataStorageTypeList) {
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

TEMPLATE_LIST_TEST_CASE(
        "Scheduler state and addr",
        "[storage]",
        spider::test::MetadataStorageTypeList
) {
    std::unique_ptr<spider::core::MetadataStorage> storage
            = spider::test::create_metadata_storage<TestType>();

    boost::uuids::random_generator gen;
    boost::uuids::uuid const scheduler_id = gen();
    constexpr int cPort = 3306;

    // Add scheduler should succeed
    REQUIRE(storage->add_driver(scheduler_id, "127.0.0.1", cPort).success());

    // Get scheduler addr should succeed
    std::string addr_res;
    int port_res = 0;
    REQUIRE(storage->get_scheduler_addr(scheduler_id, &addr_res, &port_res).success());
    REQUIRE(addr_res == "127.0.0.1");
    REQUIRE(port_res == cPort);

    // Get non-exist scheduler should fail
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == storage->get_scheduler_addr(gen(), &addr_res, &port_res).type);

    // Get default state
    std::string state_res;
    REQUIRE(storage->get_scheduler_state(scheduler_id, &state_res).success());
    REQUIRE(state_res == "normal");
    state_res.clear();

    // Update scheduler state should succeed
    std::string state = "recovery";
    REQUIRE(storage->set_scheduler_state(scheduler_id, state).success());

    // Get new state
    REQUIRE(storage->get_scheduler_state(scheduler_id, &state_res).success());
    REQUIRE(state_res == state);
}

}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
