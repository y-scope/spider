#ifndef SPIDER_STORAGE_STORAGECONNECTION_HPP
#define SPIDER_STORAGE_STORAGECONNECTION_HPP

namespace spider::core {
class StorageConnection {
public:
    StorageConnection() = default;
    StorageConnection(StorageConnection const&) = delete;
    auto operator=(StorageConnection const&) -> StorageConnection& = delete;
    StorageConnection(StorageConnection&&) = default;
    auto operator=(StorageConnection&&) -> StorageConnection& = default;
    virtual ~StorageConnection() = default;
};
}  // namespace spider::core

#endif
