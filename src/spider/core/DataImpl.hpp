#ifndef SPIDER_CORE_DATAIMPL_HPP
#define SPIDER_CORE_DATAIMPL_HPP

#include <memory>
#include <utility>

#include "../client/Data.hpp"
#include "../storage/StorageFactory.hpp"
#include "Context.hpp"
#include "Data.hpp"

namespace spider::core {
class DataImpl {
public:
    template <class T>
    static auto create_data(
            std::unique_ptr<Data> data,
            Context const& context,
            std::shared_ptr<DataStorage> data_store,
            std::shared_ptr<StorageFactory> storage_factory
    ) -> spider::Data<T> {
        return spider::Data<T>{std::move(data), context, data_store, storage_factory};
    }

    template <class T>
    static auto get_impl(spider::Data<T> const& data) -> std::unique_ptr<Data> const& {
        return data.get_impl();
    }
};
}  // namespace spider::core

#endif
