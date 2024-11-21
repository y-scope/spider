#include "Driver.hpp"

#include <boost/uuid/uuid.hpp>

namespace spider {

class DriverImpl {
public:
    DriverImpl() = default;

private:
    boost::uuids::uuid m_id;
};

Driver::Driver(std::string const& /*url*/) {}

}  // namespace spider
