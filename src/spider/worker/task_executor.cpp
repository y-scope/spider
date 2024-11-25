
#include <string>
#include <vector>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

#include "../core/MsgPack.hpp"
#include "DllLoader.hpp"
#include "FunctionManager.hpp"

namespace {

auto parse_arg(int const argc, char** const& argv) -> boost::program_options::variables_map {
    boost::program_options::options_description desc;
    desc.add_options()("help", "spider task executor")("func", boost::program_options::value<std::string>(), "function to run")(
            "libs",
            boost::program_options::value<std::vector<std::string>>(),
            "dynamic libraries that include the spider tasks"
    );

    boost::program_options::variables_map variables;
    boost::program_options::store(
            boost::program_options::parse_command_line(argc, argv, desc),
            variables
    );
    boost::program_options::notify(variables);
    return variables;
}

}  // namespace

auto main(int const argc, char** argv) -> int {
    boost::program_options::variables_map const args = parse_arg(argc, argv);

    if (!args.contains("func")) {
        return 1;
    }
    std::string const func_name = args["func"].as<std::string>();
    if (!args.contains("libs")) {
        return 1;
    }
    std::vector<std::string> const libs = args["libs"].as<std::vector<std::string>>();
    spider::worker::DllLoader& dll_loader = spider::worker::DllLoader::get_instance();
    for (std::string const& lib : libs) {
        if (false == dll_loader.load_dll(lib)) {
            return 2;
        }
    }

    // Get args buffer from stdin
    msgpack::sbuffer args_buffer{};
    // TODO: read input

    // Run fucntion
    spider::core::Function* function
            = spider::core::FunctionManager::get_instance().get_function(func_name);
    msgpack::sbuffer const result_buffer = (*function)(args_buffer);

    // Write arg buffer to stdout
}
