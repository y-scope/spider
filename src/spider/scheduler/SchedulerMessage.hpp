#ifndef SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP
#define SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP

#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include <boost/uuid/uuid.hpp>

#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp"  // IWYU pragma: keep

namespace spider::scheduler {

class ScheduleTaskRequest {
public:
    /**
     * Default constructor for msgpack. Do __not__ use it directly.
     */
    ScheduleTaskRequest() = default;

    ScheduleTaskRequest(boost::uuids::uuid const worker_id, std::string addr)
            : m_worker_id{worker_id},
              m_worker_addr{std::move(addr)} {}

    ScheduleTaskRequest(
            boost::uuids::uuid const worker_id,
            std::string addr,
            boost::uuids::uuid const task_id
    )
            : m_worker_id{worker_id},
              m_worker_addr{std::move(addr)},
              m_task_id{task_id} {}

    [[nodiscard]] auto has_task_id() const -> bool { return m_task_id.has_value(); }

    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    [[nodiscard]] auto get_task_id() const -> boost::uuids::uuid { return m_task_id.value(); }

    [[nodiscard]] auto get_worker_id() const -> boost::uuids::uuid { return m_worker_id; }

    [[nodiscard]] auto get_worker_addr() const -> std::string const& { return m_worker_addr; }

    MSGPACK_DEFINE_ARRAY(m_worker_id, m_worker_addr, m_task_id);

private:
    boost::uuids::uuid m_worker_id;
    std::string m_worker_addr;
    // Optional task id if the task fails
    std::optional<boost::uuids::uuid> m_task_id = std::nullopt;
};

class ScheduleTaskResponse {
public:
    ScheduleTaskResponse() = default;

    ScheduleTaskResponse(
            boost::uuids::uuid const task_id,
            boost::uuids::uuid const task_instance_id
    )
            : m_task_ids{std::make_tuple(task_id, task_instance_id)} {}

    explicit ScheduleTaskResponse(std::tuple<boost::uuids::uuid, boost::uuids::uuid> const& task_ids
    )
            : m_task_ids{task_ids} {}

    [[nodiscard]] auto has_task_id() const -> bool { return m_task_ids.has_value(); }

    [[nodiscard]] auto get_task_ids(
    ) const -> std::tuple<boost::uuids::uuid, boost::uuids::uuid> const& {
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        return m_task_ids.value();
    }

    MSGPACK_DEFINE_ARRAY(m_task_ids);

private:
    std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> m_task_ids = std::nullopt;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP
