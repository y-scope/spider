#ifndef SPIDER_UTILS_TIMEDCACHE_HPP
#define SPIDER_UTILS_TIMEDCACHE_HPP

#include <cstddef>
#include <optional>
#include <utility>

#include <absl/container/flat_hash_map.h>

namespace spider::core {

namespace utils {
constexpr size_t cDefaultCacheSize = 100;
}  // namespace utils

template <class Key, class Value>
class LruCache {
public:
    LruCache() = default;

    explicit LruCache(size_t const size) : m_size{size} {}

    auto get(Key const& key) -> std::optional<Value> {
        auto it = m_map.find(key);
        if (it == m_map.end()) {
            return std::nullopt;
        }

        return it->second->second;
    }

    auto put(Key const& key, Value const& value) -> void {
        auto it = m_map.find(key);
        if (it != m_map.end()) {
            update(it, key, value);
            return;
        }
        // Pop if the size is greater than the threshold
        if (m_map.size() >= m_size) {
            std::pair<Key, Value>& last = m_list.back();
            m_map.erase(last.first);
            m_list.pop_back();
        }

        m_list.push_front({key, value});
        m_map[key] = m_list.begin();
    }

private:
    auto update(
            typename absl::flat_hash_map<Key, typename std::list<std::pair<Key, Value>>::iterator>::
                    iterator& it,
            Key const& key,
            Value const& value
    ) -> void {
        auto list_it = it->second;
        m_list.erase(list_it);
        m_list.push_front({key, value});
        it->second = m_list.begin();
    }

    size_t m_size = utils::cDefaultCacheSize;
    std::list<std::pair<Key, Value>> m_list;
    absl::flat_hash_map<Key, typename std::list<std::pair<Key, Value>>::iterator> m_map;
};

}  // namespace spider::core

#endif  // SPIDER_UTILS_TIMEDCACHE_HPP
