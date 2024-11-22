#ifndef SPIDER_WORKER_DLLLOADER_HPP
#define SPIDER_WORKER_DLLLOADER_HPP

#include <absl/container/flat_hash_map.h>

#include <boost/dll/shared_library.hpp>
#include <string>

namespace spider::worker {

class DllLoader {
public:
    static auto get_instance() -> DllLoader& {
        // Explicitly use new because DllLoader instance should not be destroyed
        // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
        static auto* instance = new DllLoader();
        return *instance;
    }

    auto load_dll(std::string const& path) -> bool;

private:
    absl::flat_hash_map<std::string, boost::dll::shared_library> m_libraries;
};

}  // namespace spider::worker

#endif  // SPIDER_WORKER_DLLLOADER_HPP
