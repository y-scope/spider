#ifndef SPIDER_CORE_DATA_HPP
#define SPIDER_CORE_DATA_HPP

#include <boost/uuid/random_generator.hpp>

class Data {
private:
    boost::uuids::uuid m_id;
    std::optional<std::string> m_key;
    std::string m_value;
public:
    explicit Data(std::string value): m_value(std::move(value)) {
        boost::uuids::random_generator gen;
        m_id = gen();
    }
    Data(std::string key, std::string value): m_key(std::move(key)), m_value(std::move(value)) {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

    boost::uuids::uuid get_id() { return m_id; }
    std::optional<std::string> get_key() { return m_key; }
    std::string get_value() { return m_value; }
};

#endif  // SPIDER_CORE_DATA_HPP
