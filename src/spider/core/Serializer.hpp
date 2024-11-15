#ifndef SPIDER_CORE_SERIALIZER_HPP
#define SPIDER_CORE_SERIALIZER_HPP

#include <boost/uuid/uuid.hpp>
#include <cstdint>
#include <cstring>
#include <msgpack.hpp>

template <>
struct msgpack::adaptor::convert<boost::uuids::uuid> {
    auto operator()(msgpack::object const& object, boost::uuids::uuid& id) const
            -> msgpack::object const& {
        if (object.type != msgpack::type::BIN) {
            throw msgpack::type_error();
        }
        if (object.via.bin.size != boost::uuids::uuid::static_size()) {
            throw msgpack::type_error();
        }
        std::uint8_t data[boost::uuids::uuid::static_size()];
        std::memcpy(data, object.via.bin.ptr, boost::uuids::uuid::static_size());
        id = boost::uuids::uuid{data};

        return object;
    }
};

template <>
struct msgpack::adaptor::pack<boost::uuids::uuid> {
    template <class Stream>
    auto operator()(msgpack::packer<Stream>& packer, boost::uuids::uuid const& id) const
            -> msgpack::packer<Stream>& {
        packer.pack_bin(id.size());
        packer.pack_bin_body(id.data(), id.size());
    }
};

#endif  // SPIDER_CORE_SERIALIZER_HPP
