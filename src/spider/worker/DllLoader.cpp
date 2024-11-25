#include "DllLoader.hpp"

#include <exception>
#include <filesystem>
#include <string>

#include <spdlog/spdlog.h>

namespace spider::worker {

auto DllLoader::load_dll(std::string const& path_str) -> bool {
    std::filesystem::path const dll_path(path_str);

    if (std::filesystem::exists(dll_path)) {
        spdlog::error("Cannot find dll file {}", dll_path.string());
        return false;
    }

    try {
        m_libraries.emplace(path_str, boost::dll::shared_library{dll_path.string()});
    } catch (std::exception& e) {
        spdlog::error("Failed to load library {}: {}", dll_path.string(), e.what());
        return false;
    } catch (...) {
        spdlog::error("Failed to load library {}", dll_path.string());
        return false;
    }

    return true;
}

}  // namespace spider::worker
