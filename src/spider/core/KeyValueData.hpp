#ifndef SPIDER_CORE_KEYVALUEDATA_HPP
#define SPIDER_CORE_KEYVALUEDATA_HPP

#include <string>
#include <utility>

#include <boost/uuid/uuid.hpp>

namespace spider::core {
class KeyValueData {
public:
    KeyValueData(std::string key, std::string value, boost::uuids::uuid const id)
            : m_key{std::move(key)},
              m_value{std::move(value)},
              m_id{id} {}

    [[nodiscard]] auto get_key() const -> std::string const& { return m_key; }

    [[nodiscard]] auto get_value() const -> std::string const& { return m_value; }

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid const& { return m_id; }

private:
    std::string m_key;
    std::string m_value;
    boost::uuids::uuid m_id;
};
}  // namespace spider::core

#endif  // SPIDER_CORE_KEYVALUEDATA_HPP
