#include "FifoPolicy.hpp"

namespace spider::scheduler {

auto FifoPolicy::schedule_next(
        std::shared_ptr<core::MetadataStorage> metadata_store,
        std::shared_ptr<core::DataStorage> data_store
) -> boost::uuids::uuid {}

auto FifoPolicy::cleanup_job(boost::uuids::uuid job_id) -> void {}

}  // namespace spider::scheduler
