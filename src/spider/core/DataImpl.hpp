#ifndef SPIDER_CORE_DATAIMPL_HPP
#define SPIDER_CORE_DATAIMPL_HPP

#include <memory>

#include "../client/Data.hpp"
#include "../core/Data.hpp"

namespace spider::core {

class DataImpl {
public:
    template <class T>
    static auto create_data(std::unique_ptr<Data> data, std::shared_ptr<DataStorage> data_store)
            -> spider::Data<T> {
        return spider::Data<T>{std::move(data), data_store};
    }

    template <class T>
    static auto get_impl(spider::Data<T> const& data) -> std::shared_ptr<DataStorage> {
        return data.get_impl();
    }
};

}  // namespace spider::core

#endif
