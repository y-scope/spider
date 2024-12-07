#ifndef SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP
#define SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP

#include <optional>
#include <string>
#include <utility>

#include <boost/uuid/uuid.hpp>

#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp"  // IWYU pragma: keep

namespace spider::scheduler {

class ScheduleTaskRequest {
public:
    ScheduleTaskRequest(boost::uuids::uuid const worker_id, std::string addr)
            : m_worker_id{worker_id},
              m_worker_addr{std::move(addr)} {}

    ScheduleTaskRequest(
            boost::uuids::uuid const task_id,
            boost::uuids::uuid const task_instance_id,
            boost::uuids::uuid const worker_id,
            std::string addr
    )
            : m_worker_id{worker_id},
              m_worker_addr{std::move(addr)},
              m_task{std::pair{task_id, task_instance_id}} {}

    [[nodiscard]] auto get_worker_id() const -> boost::uuids::uuid { return m_worker_id; }

    [[nodiscard]] auto get_worker_addr() const -> std::string const& { return m_worker_addr; }

    [[nodiscard]] auto is_task_complete() const -> bool { return m_task.has_value(); }

    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    [[nodiscard]] auto get_task_id() const -> boost::uuids::uuid { return m_task.value().first; }

    [[nodiscard]] auto get_task_instance_id() const -> boost::uuids::uuid {
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        return m_task.value().second;
    }

    MSGPACK_DEFINE_ARRAY(m_worker_id, m_worker_addr, m_task);

private:
    boost::uuids::uuid m_worker_id;
    std::string m_worker_addr;
    // pair.first is task id, pair.second is task instance id
    std::optional<std::pair<boost::uuids::uuid, boost::uuids::uuid>> m_task = std::nullopt;
};

class ScheduleTaskResponse {
public:
    explicit ScheduleTaskResponse(boost::uuids::uuid const task_id) : m_task_id{task_id} {}

    [[nodiscard]] auto get_task_id() const -> boost::uuids::uuid { return m_task_id; }

    MSGPACK_DEFINE_ARRAY(m_task_id);

private:
    boost::uuids::uuid m_task_id;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP
