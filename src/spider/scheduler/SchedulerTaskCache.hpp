#ifndef SPIDER_SCHEDULER_SCHEDULERTASKCACHE_HPP
#define SPIDER_SCHEDULER_SCHEDULERTASKCACHE_HPP

#include <memory>

#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"

namespace spider::core {

class SchedulerTaskCache {
public:
    SchedulerTaskCache(
            std::shared_ptr<MetadataStorage> const& metadata_store,
            std::shared_ptr<DataStorage> const& data_store
    )
            : m_metadata_store{metadata_store},
              m_data_store{data_store} {}

private:
    std::shared_ptr<MetadataStorage> m_metadata_store;
    std::shared_ptr<DataStorage> m_data_store;
};

}  // namespace spider::core

#endif  // SPIDER_SCHEDULER_SCHEDULERTASKCACHE_HPP
