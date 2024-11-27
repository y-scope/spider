
#include <unistd.h>

#include <exception>
#include <optional>
#include <string>
#include <vector>

#include <boost/any/bad_any_cast.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "../core/BoostAsio.hpp"  // IWYU pragma: keep
#include "../core/MsgPack.hpp"  // IWYU pragma: keep
#include "DllLoader.hpp"
#include "FunctionManager.hpp"
#include "message_pipe.hpp"
#include "TaskExecutorMessage.hpp"

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
    // Set up spdlog to write to stderr
    spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));

    boost::program_options::variables_map const args = parse_arg(argc, argv);

    std::string func_name;
    try {
        if (!args.contains("func")) {
            return 1;
        }
        func_name = args["func"].as<std::string>();
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
    } catch (boost::bad_any_cast& e) {
        return 1;
    }

    try {
        // Set up asio
        boost::asio::io_context context;
        boost::asio::posix::stream_descriptor in(context, dup(STDIN_FILENO));
        boost::asio::posix::stream_descriptor out(context, dup(STDOUT_FILENO));

        // Get args buffer from stdin
        std::optional<msgpack::sbuffer> request_buffer_option = spider::worker::receive_message(in);
        if (!request_buffer_option.has_value()) {
            spdlog::error("Cannot read args buffer request");
            return 3;
        }
        msgpack::sbuffer const& request_buffer = request_buffer_option.value();
        if (spider::worker::TaskExecutorRequestType::Arguments
            != spider::worker::get_request_type(request_buffer))
        {
            spdlog::error("Expect args request.");
            return 3;
        }
        msgpack::object const args_object = spider::worker::get_message_body(request_buffer);
        msgpack::sbuffer args_buffer;
        msgpack::packer packer{args_buffer};
        packer.pack(args_object);

        // Run function
        spider::core::Function const* function
                = spider::core::FunctionManager::get_instance().get_function(func_name);
        if (nullptr == function) {
            spider::worker::send_message(
                    out,
                    spider::core::create_error_response(
                            spider::core::FunctionInvokeError::FunctionExecutionError,
                            fmt::format("Function {} not found.", func_name)
                    )
            );
            return 4;
        }
        msgpack::sbuffer const result_buffer = (*function)(args_buffer);

        // Write result buffer to stdout
        spider::worker::send_message(out, result_buffer);
    } catch (std::exception& e) {
        spdlog::error("Exception thrown: {}", e.what());
        return 5;
    }
    return 0;
}
