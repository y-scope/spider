#ifndef SPIDER_CLIENT_SPIDER_HPP
#define SPIDER_CLIENT_SPIDER_HPP

#include <functional>
#include <string>

// NOLINTBEGIN(misc-include-cleaner)
#include "Data.hpp"
#include "Future.hpp"
#include "Task.hpp"
#include "TaskGraph.hpp"

// NOLINTEND(misc-include-cleaner)

namespace spider {
/**
 * Initializes Spider library
 */
void init();

/**
 * Connects to storage
 * @param url url of the storage to connect
 */
void connect(std::string const& url);

/**
 * Registers function to Spider
 * @param function function to register
 */
template <class R, class... Args>
void register_task(std::function<R(Args...)> const& function);

/**
 * Registers function to Spider with timeout
 * @param function_name name of the function to register
 * @param timeout task is considered straggler after timeout ms, and Spider triggers replicate the
 * task
 */
template <class R, class... Args>
void register_task(std::function<R(Args...)> const& function, float timeout);
}  // namespace spider

#endif  // SPIDER_CLIENT_SPIDER_HPP
