#ifndef SPIDER_TEST_WORKER_TEST_HPP
#define SPIDER_TEST_WORKER_TEST_HPP

#include <string>
#include <tuple>

#include <spider/client/Data.hpp>
#include <spider/client/TaskContext.hpp>

auto sum_test(spider::TaskContext& /*context*/, int x, int y) -> int;

auto swap_test(spider::TaskContext& /*context*/, int x, int y) -> std::tuple<int, int>;

auto error_test(spider::TaskContext& /*context*/, int /*x*/) -> int;

auto data_test(spider::TaskContext& /*context*/, spider::Data<int>& data) -> int;

auto random_fail_test(spider::TaskContext& /*context*/, int fail_rate) -> int;

auto create_data_test(spider::TaskContext& context, int x) -> spider::Data<int>;

auto create_task_test(spider::TaskContext& context, int x, int y) -> int;

auto join_string_test(
        spider::TaskContext& context,
        std::string const& input_1,
        std::string const& input_2
) -> std::string;

auto sleep_test(spider::TaskContext& context, int milliseconds) -> int;

auto abort_test(spider::TaskContext& context, int x) -> int;

#endif
