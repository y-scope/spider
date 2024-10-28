#ifndef SPIDER_CORE_DATA_HPP
#define SPIDER_CORE_DATA_HPP

#include <boost/uuid/random_generator.hpp>

class Data {
private:
    boost::uuids::uuid m_id;
    std::optional<std::string> m_key;
    std::string m_value;
    bool m_hard_locality = false;
    std::vector<std::string> m_locality;

public:
    explicit Data(std::string value) : m_value(std::move(value)) {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

    Data(boost::uuids::uuid id, std::string value) : m_id(id), m_value(value) {}

    Data(std::string key, std::string value) : m_key(std::move(key)), m_value(std::move(value)) {
        boost::uuids::random_generator gen;
        m_id = gen();
    }

    Data(boost::uuids::uuid id, std::string key, std::string value)
            : m_id(id),
              m_key(key),
              m_value(value) {}

    boost::uuids::uuid get_id() const { return m_id; }

    std::optional<std::string> get_key() const { return m_key; }

    std::string get_value() const { return m_value; }

    bool is_hard_locality() const { return m_hard_locality; }

    void set_hard_locality(bool is_hard_locality) { m_hard_locality = is_hard_locality; }

    std::vector<std::string> get_locality() const { return m_locality; }

    void set_locality(std::vector<std::string> locality) { m_locality = std::move(locality); }
};

#endif  // SPIDER_CORE_DATA_HPP
