#ifndef SPIDER_SCHEDULER_FIFOPOLICY_HPP
#define SPIDER_SCHEDULER_FIFOPOLICY_HPP

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "../core/Task.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"
#include "SchedulerPolicy.hpp"

namespace spider::scheduler {

class FifoPolicy final : public SchedulerPolicy {
public:
    FifoPolicy(
            std::shared_ptr<core::MetadataStorage> const& metadata_store,
            std::shared_ptr<core::DataStorage> const& data_store,
            core::StorageConnection& conn
    );

    auto schedule_next(boost::uuids::uuid worker_id, std::string const& worker_addr)
            -> std::optional<boost::uuids::uuid> override;

private:
    auto fetch_tasks() -> void;

    std::shared_ptr<core::MetadataStorage> m_metadata_store;
    std::shared_ptr<core::DataStorage> m_data_store;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    core::StorageConnection& m_conn;

    std::vector<core::Task> m_tasks;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_FIFOPOLICY_HPP
