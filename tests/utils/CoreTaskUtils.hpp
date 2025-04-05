#ifndef SPIDER_TESTS_CORETASKUTILS_HPP
#define SPIDER_TESTS_CORETASKUTILS_HPP

#include "../../src/spider/core/Task.hpp"
#include "../../src/spider/core/TaskGraph.hpp"

namespace spider::test {
auto task_equal(core::Task const& t1, core::Task const& t2) -> bool;

auto task_input_equal(core::TaskInput const& input_1, core::TaskInput const& input_2) -> bool;

auto task_output_equal(core::TaskOutput const& output_1, core::TaskOutput const& output_2) -> bool;

auto task_graph_equal(core::TaskGraph const& graph_1, core::TaskGraph const& graph_2) -> bool;
}  // namespace spider::test

#endif  // SPIDER_TESTS_CORETASKUTILS_HPP
