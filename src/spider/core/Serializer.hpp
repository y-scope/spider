#ifndef SPIDER_CORE_SERIALIZER_HPP
#define SPIDER_CORE_SERIALIZER_HPP

#include <boost/uuid/uuid.hpp>
#include <cstdint>
#include <cstring>

#include "Data.hpp"
#include "MsgPack.hpp"  // IWYU pragma: keep

template <>
struct msgpack::adaptor::convert<boost::uuids::uuid> {
    auto operator()(msgpack::object const& object, boost::uuids::uuid& id) const
            -> msgpack::object const& {
        // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay,bugprone-return-const-ref-from-parameter)
        if (object.type != type::BIN) {
            throw type_error();
        }
        if (object.via.bin.size != boost::uuids::uuid::static_size()) {
            throw type_error();
        }
        std::uint8_t data[boost::uuids::uuid::static_size()];
        std::memcpy(data, object.via.bin.ptr, boost::uuids::uuid::static_size());
        id = boost::uuids::uuid{data};

        return object;
        // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay,bugprone-return-const-ref-from-parameter)
    }
};

template <>
struct msgpack::adaptor::pack<boost::uuids::uuid> {
    template <class Stream>
    auto operator()(msgpack::packer<Stream>& packer, boost::uuids::uuid const& id) const
            -> msgpack::packer<Stream>& {
        packer.pack_bin(id.size());
        // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
        packer.pack_bin_body((char const*)id.data(), id.size());
        // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
        return packer;
    }
};

#endif  // SPIDER_CORE_SERIALIZER_HPP
