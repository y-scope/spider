#ifndef TASKS_HPP
#define TASKS_HPP

#include <spider/client/spider.hpp>

/**
 * @param context
 * @param value
 * @return The square of the given value.
 */
auto square(spider::TaskContext& context, int value) -> int;

/**
 * @param context
 * @param value
 * @return The square root of the given value.
 */
auto square_root(spider::TaskContext& context, int value) -> double;

/**
 * @param context
 * @param x
 * @param y
 * @return The sum of x and y.
 */
auto sum(spider::TaskContext& context, int x, int y) -> int;

#endif  // TASKS_HPP
