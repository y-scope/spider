
#include <unistd.h>

#include <string>
#include <vector>

#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

#include "../core/MsgPack.hpp"
#include "DllLoader.hpp"
#include "FunctionManager.hpp"
#include "message_pipe.hpp"

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
            // NOLINTNEXTLINE(misc-include-cleaner)
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

    // Set up asio
    boost::asio::io_context context;
    boost::asio::posix::stream_descriptor in(context, dup(STDIN_FILENO));
    boost::asio::posix::stream_descriptor out(context, dup(STDOUT_FILENO));

    // Get args buffer from stdin
    std::optional<msgpack::sbuffer> request_buffer_option = spider::worker::receive_message(in);
    if (!request_buffer_option.has_value()) {
        return 3;
    }
    msgpack::sbuffer const& request_buffer = request_buffer_option.value();
    if (spider::worker::TaskExecutorRequestType::Arguments
        == spider::worker::get_request_type(request_buffer))
    {
        return 3;
    }
    msgpack::sbuffer const args_buffer = spider::worker::get_request_body(request_buffer);

    // Run function
    spider::core::Function* function
            = spider::core::FunctionManager::get_instance().get_function(func_name);
    msgpack::sbuffer const result_buffer = (*function)(args_buffer);

    // Write arg buffer to stdout
    msgpack::sbuffer const response_buffer = spider::core::create_result_response(result_buffer);
    spider::worker::send_message(out, response_buffer);
}
