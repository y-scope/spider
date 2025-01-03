#ifndef SPIDER_CORE_JOBMETADATA_HPP
#define SPIDER_CORE_JOBMETADATA_HPP

#include <chrono>
#include <cstdint>

#include <boost/uuid/uuid.hpp>

namespace spider::core {

class JobMetadata {
public:
    JobMetadata() = default;

    JobMetadata(
            boost::uuids::uuid id,
            boost::uuids::uuid client_id,
            std::chrono::system_clock::time_point creation_time
    )
            : m_id{id},
              m_client_id{client_id},
              m_creation_time{creation_time} {}

    [[nodiscard]] auto get_id() const -> boost::uuids::uuid { return m_id; }

    [[nodiscard]] auto get_client_id() const -> boost::uuids::uuid { return m_client_id; }

    [[nodiscard]] auto get_creation_time() const -> std::chrono::system_clock::time_point {
        return m_creation_time;
    }

private:
    boost::uuids::uuid m_id;
    boost::uuids::uuid m_client_id;
    std::chrono::system_clock::time_point m_creation_time;
};

enum class JobStatus : std::uint8_t {
    Running,
    Succeeded,
    Failed,
    Cancelled
};

}  // namespace spider::core

#endif  // SPIDER_CORE_JOBMETADATA_HPP
