#ifndef SPIDER_UTILS_TIMEDCACHE_HPP
#define SPIDER_UTILS_TIMEDCACHE_HPP

#include <chrono>
#include <optional>
#include <utility>

#include <absl/container/flat_hash_map.h>

namespace spider::core {

constexpr unsigned cDefaultThreshold = 5;

template <class Key, class Value>
class TimedCache {
public:
    TimedCache() = default;

    explicit TimedCache(unsigned const seconds) : m_duration{std::chrono::seconds(seconds)} {}

    auto get(Key const& key) -> std::optional<Value> {
        auto iter = m_map.find(key);
        if (iter == m_map.end()) {
            return std::nullopt;
        }
        iter->second.first = std::chrono::steady_clock::now();
        return iter->second.second;
    }

    auto put(Key const& key, Value const& value) {
        auto iter = m_map.find(key);
        if (iter == m_map.end()) {
            m_map[key] = std::make_pair(std::chrono::steady_clock::now(), value);
        } else {
            iter->second.first = std::chrono::steady_clock::now();
            iter->second.second = value;
        }
    }

    auto cleanup() {
        erase_if(m_map, [&](auto const& item) -> bool {
            auto const& [key, value] = item;
            return std::chrono::steady_clock::now() - value.first > m_duration;
        });
    }

private:
    std::chrono::steady_clock::duration m_duration = std::chrono::seconds(cDefaultThreshold);
    absl::flat_hash_map<Key, std::pair<std::chrono::steady_clock::time_point, Value>> m_map;
};

}  // namespace spider::core

#endif  // SPIDER_UTILS_TIMEDCACHE_HPP
