#ifndef SPIDER_CORE_DATA_HPP
#define SPIDER_CORE_DATA_HPP

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <optional>
#include <string>
#include <utility>

class Data {
private:
    boost::uuids::uuid m_id;
    std::optional<std::string> m_key;
    std::string m_value;

public:
    explicit Data(std::string value) : m_value(std::move(value)) {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

    Data(std::string key, std::string value) : m_key(std::move(key)), m_value(std::move(value)) {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

    auto get_id() -> boost::uuids::uuid { return m_id; }

    auto get_key() -> std::optional<std::string> { return m_key; }

    auto get_value() -> std::string { return m_value; }
};

#endif  // SPIDER_CORE_DATA_HPP
