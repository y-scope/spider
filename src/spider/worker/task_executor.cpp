
#include <unistd.h>

#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/any/bad_any_cast.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>  // IWYU pragma: keep
#include <spdlog/spdlog.h>

#include "../client/TaskContext.hpp"
#include "../core/Error.hpp"
#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/MysqlStorage.hpp"
#include "DllLoader.hpp"
#include "FunctionManager.hpp"
#include "message_pipe.hpp"
#include "TaskExecutorMessage.hpp"

namespace {

auto parse_arg(int const argc, char** const& argv) -> boost::program_options::variables_map {
    boost::program_options::options_description desc;
    desc.add_options()("help", "spider task executor");
    desc.add_options()("func", boost::program_options::value<std::string>(), "function to run");
    desc.add_options()(
            "libs",
            boost::program_options::value<std::vector<std::string>>(),
            "dynamic libraries that include the spider tasks"
    );
    desc.add_options()(
            "storage_url",
            boost::program_options::value<std::string>(),
            "storage server url"
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

constexpr int cCmdArgParseErr = 1;
constexpr int cStorageErr = 2;
constexpr int cDllErr = 3;
constexpr int cFuncArgParseErr = 4;
constexpr int cResultSendErr = 5;
constexpr int cOtherErr = 6;

auto main(int const argc, char** argv) -> int {
    // Set up spdlog to write to stderr
    // NOLINTNEXTLINE(misc-include-cleaner)
    spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [spider.executor] %v");
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#endif

    boost::program_options::variables_map const args = parse_arg(argc, argv);

    std::string func_name;
    std::string storage_url;
    try {
        if (!args.contains("func")) {
            return cCmdArgParseErr;
        }
        func_name = args["func"].as<std::string>();
        if (!args.contains("storage_url")) {
            return cCmdArgParseErr;
        }
        storage_url = args["storage_url"].as<std::string>();
        if (!args.contains("libs")) {
            return cCmdArgParseErr;
        }
        std::vector<std::string> const libs = args["libs"].as<std::vector<std::string>>();
        spider::worker::DllLoader& dll_loader = spider::worker::DllLoader::get_instance();
        for (std::string const& lib : libs) {
            if (false == dll_loader.load_dll(lib)) {
                return cDllErr;
            }
        }
    } catch (boost::bad_any_cast& e) {
        return cCmdArgParseErr;
    } catch (boost::program_options::error& e) {
        return cCmdArgParseErr;
    }

    spdlog::debug("Function to run: {}", func_name);

    try {
        // Set up storage
        std::shared_ptr<spider::core::MetadataStorage> const metadata_store
                = std::make_shared<spider::core::MySqlMetadataStorage>();
        std::shared_ptr<spider::core::DataStorage> const data_store
                = std::make_shared<spider::core::MySqlDataStorage>();
        spider::core::StorageErr err = metadata_store->connect(storage_url);
        if (!err.success()) {
            spdlog::error("Failed to connect to storage {}", storage_url);
            return cStorageErr;
        }
        err = data_store->connect(storage_url);
        if (!err.success()) {
            spdlog::error("Failed to connect to storage {}", storage_url);
            return cStorageErr;
        }

        // Set up asio
        boost::asio::io_context context;
        boost::asio::posix::stream_descriptor in(context, dup(STDIN_FILENO));
        boost::asio::posix::stream_descriptor out(context, dup(STDOUT_FILENO));

        // Get args buffer from stdin
        std::optional<msgpack::sbuffer> request_buffer_option = spider::worker::receive_message(in);
        if (!request_buffer_option.has_value()) {
            spdlog::error("Cannot read args buffer request");
            return cFuncArgParseErr;
        }
        msgpack::sbuffer const& request_buffer = request_buffer_option.value();
        if (spider::worker::TaskExecutorRequestType::Arguments
            != spider::worker::get_request_type(request_buffer))
        {
            spdlog::error("Expect args request.");
            return cFuncArgParseErr;
        }
        msgpack::object const args_object = spider::worker::get_message_body(request_buffer);
        msgpack::sbuffer args_buffer;
        msgpack::packer packer{args_buffer};
        packer.pack(args_object);
        spdlog::debug("Args buffer parsed");

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
            return cResultSendErr;
        }
        spider::TaskContext const task_context
                = spider::core::TaskContextImpl::create_task_context(data_store, metadata_store);
        msgpack::sbuffer const result_buffer = (*function)(task_context, args_buffer);
        spdlog::debug("Function executed");

        // Write result buffer to stdout
        spider::worker::send_message(out, result_buffer);
    } catch (std::exception& e) {
        spdlog::error("Exception thrown: {}", e.what());
        return cOtherErr;
    }
    return 0;
}
