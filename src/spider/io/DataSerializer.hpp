#ifndef SPIDER_CORE_DATASEIALIZER_HPP
#define SPIDER_CORE_DATASEIALIZER_HPP

#include <boost/uuid/uuid.hpp>

#include "../client/Data.hpp"
#include "MsgPack.hpp"  // IWYU pragma: keep
#include "Serializer.hpp"  // IWYU pragma: keep

namespace spider::core {
class DataSerializer {
public:
    template <class Stream, class T>
    static auto serialize_id(msgpack::packer<Stream>& packer, spider::Data<T> const& data) -> void {
        packer.pack(data.get_impl()->get_id());
    }

    template <class T>
    static auto data_get_id(spider::Data<T> const& data) -> boost::uuids::uuid {
        return data.get_impl()->get_id();
    }
};

}  // namespace spider::core

#endif
