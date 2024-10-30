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

    void init_id() {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

public:
    explicit Data(std::string value) : m_value(std::move(value)) { init_id(); }

    Data(std::string key, std::string value) : m_key(std::move(key)), m_value(std::move(value)) {
        init_id();
    }

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid { return m_id; }

    [[nodiscard]] auto get_key() const -> std::optional<std::string> { return m_key; }

    [[nodiscard]] auto get_value() const -> std::string { return m_value; }
};

#endif  // SPIDER_CORE_DATA_HPP
