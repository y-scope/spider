#ifndef SPIDER_CLIENT_DATASERIALIZER_HPP
#define SPIDER_CLIENT_DATASERIALIZER_HPP

#include "../client/Data.hpp"
#include "../core/Data.hpp"  // IWYU pragma: keep
#include "MsgPack.hpp"  // IWYU pragma: keep

template <class T>
struct msgpack::adaptor::pack<spider::Data<T>> {
    template <class Stream>
    auto operator()(msgpack::packer<Stream>& packer, spider::Data<T> const& data) const
            -> msgpack::packer<Stream>& {
        packer.pack_map(1);
        packer.pack(data.get_impl()->get_id());
        return packer;
    }
};

#endif
