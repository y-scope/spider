#ifndef TASKS_HPP
#define TASKS_HPP

#include <spider/client/spider.hpp>

// Task function prototype
/**
 * @param context
 * @param x
 * @param y
 * @return The sum of x and y.
 */
auto sum(spider::TaskContext& context, int x, int y) -> int;

#endif  // TASKS_HPP
