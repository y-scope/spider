#include "Job.hpp"

#include <boost/uuid/uuid.hpp>
#include <string>
#include <utility>

namespace spider {

class JobImpl {
    // Implementation details subject to change
public:
    auto get_status() -> JobStatus {
        if (m_id.is_nil()) {
            return JobStatus::Cancel;
        }
        return JobStatus::Running;
    }

private:
    boost::uuids::uuid m_id;
};

template <class T>
auto Job<T>::wait_complete() {}

template <class T>
auto Job<T>::get_status() -> JobStatus {
    return m_impl->get_status();
}

template <class T>
auto Job<T>::get_result() -> T {
    return T{};
}

template <class T>
auto Job<T>::get_error() -> std::pair<std::string, std::string> {
    return std::make_pair("", "");
}

}  // namespace spider
