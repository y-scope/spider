#ifndef SPIDER_TEST_WORKER_TEST_HPP
#define SPIDER_TEST_WORKER_TEST_HPP

#include "../../src/spider/client/Data.hpp"
#include "../../src/spider/client/TaskContext.hpp"

auto sum_test(spider::TaskContext& /*context*/, int x, int y) -> int;

auto error_test(spider::TaskContext& /*context*/, int /*x*/) -> int;

auto data_test(spider::TaskContext& /*context*/, spider::Data<int>& data) -> int;

auto random_fail_test(spider::TaskContext& /*context*/, int fail_rate) -> int;

#endif
