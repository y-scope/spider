#ifndef SPIDER_CLIENT_SPIDER_HPP
#define SPIDER_CLIENT_SPIDER_HPP

#include "Error.hpp"
#include "Task.hpp"

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

}  // namespace spider

#endif  // SPIDER_CLIENT_SPIDER_HPP
