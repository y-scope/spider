#ifndef SPIDER_CLIENT_KEYVALUEDATA_HPP
#define SPIDER_CLIENT_KEYVALUEDATA_HPP

#include <optional>
#include <string>

namespace spider {

/**
 * Insert the key-value pair into the key value store. Overwrite the existing value stored if key
 * already exists.
 * @param key key of the key-value pair
 * @param value value of the key-value pair
 */
auto insert_kv(std::string const& key, std::string const& value);

/**
 * Get the value based on the key. Client can only get the value created by itself.
 * @param key key to lookup
 * @return std::nullopt if key not in storage, corresponding value if key in storage
 */
auto get_kv(std::string const& key) -> std::optional<std::string>;

}  // namespace spider
#endif  // SPIDER_CLIENT_KEYVALUEDATA_HPP
