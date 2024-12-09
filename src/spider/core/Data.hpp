#ifndef SPIDER_CORE_DATA_HPP
#define SPIDER_CORE_DATA_HPP

#include <string>
#include <utility>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>

namespace spider::core {
class Data {
public:
    Data() { init_id(); }

    explicit Data(std::string value) : m_value(std::move(value)) { init_id(); }

    Data(boost::uuids::uuid const id, std::string value) : m_id(id), m_value(std::move(value)) {}

    static auto is_data() -> bool { return true; }

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid { return m_id; }

    [[nodiscard]] auto get_value() const -> std::string { return m_value; }

    [[nodiscard]] auto get_locality() const -> std::vector<std::string> const& {
        return m_locality;
    }

    [[nodiscard]] auto is_hard_locality() const -> bool { return m_hard_locality; }

    void set_locality(std::vector<std::string> const& locality) { m_locality = locality; }

    void set_hard_locality(bool const hard) { m_hard_locality = hard; }

private:
    boost::uuids::uuid m_id;
    std::string m_value;
    std::vector<std::string> m_locality;
    bool m_hard_locality = false;

    void init_id() {
        boost::uuids::random_generator gen;
        m_id = gen();
    }
};

}  // namespace spider::core

#endif  // SPIDER_CORE_DATA_HPP
