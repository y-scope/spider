#include "DllLoader.hpp"

#include <exception>
#include <filesystem>
#include <string>

#include <boost/dll/import.hpp>
#include <boost/dll/shared_library.hpp>
#include <spdlog/spdlog.h>

#include "../worker/FunctionManager.hpp"

namespace spider::worker {

auto DllLoader::load_dll(std::string const& path_str) -> bool {
    std::filesystem::path const dll_path(path_str);

    if (!std::filesystem::exists(dll_path)) {
        spdlog::error("Cannot find dll file {}", dll_path.string());
        return false;
    }

    try {
        boost::dll::shared_library library{dll_path.string()};

        auto const function_manager_func = boost::dll::import_alias<core::FunctionManager&()>(
                library,
                "function_manager_get_instance"
        );
        core::FunctionManager const& function_manager = function_manager_func();
        core::FunctionMap const& function_map = function_manager.get_function_map();
        for (auto const& func_iter : function_map) {
            core::FunctionManager::get_instance().register_function_invoker(
                    func_iter.first,
                    func_iter.second
            );
        }

        m_libraries.emplace(path_str, library);
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
