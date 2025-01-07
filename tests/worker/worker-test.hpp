#ifndef SPIDER_TEST_WORKER_TEST_HPP
#define SPIDER_TEST_WORKER_TEST_HPP

#include <spider/client/Data.hpp>
#include <spider/client/TaskContext.hpp>

auto sum_test(spider::TaskContext& /*context*/, int x, int y) -> int;

auto error_test(spider::TaskContext& /*context*/, int /*x*/) -> int;

auto data_test(spider::TaskContext& /*context*/, spider::Data<int>& data) -> int;

auto random_fail_test(spider::TaskContext& /*context*/, int fail_rate) -> int;

auto create_data_test(spider::TaskContext& context, int x) -> spider::Data<int>;

auto create_task_test(spider::TaskContext& context, int x, int y) -> int;

#endif
