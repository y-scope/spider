#include <cerrno>
#include <csignal>
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
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>  // IWYU pragma: keep
#include <spdlog/spdlog.h>

#include <spider/client/TaskContext.hpp>
#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/mysql/MySqlStorageFactory.hpp>
#include <spider/storage/StorageFactory.hpp>
#include <spider/worker/DllLoader.hpp>
#include <spider/worker/FunctionManager.hpp>
#include <spider/worker/message_pipe.hpp>
#include <spider/worker/TaskExecutorMessage.hpp>

namespace {
auto parse_arg(int const argc, char** const& argv) -> boost::program_options::variables_map {
    boost::program_options::options_description desc;
    desc.add_options()("help", "spider task executor");
    desc.add_options()("func", boost::program_options::value<std::string>(), "function to run");
    desc.add_options()(
            "task_id",
            boost::program_options::value<std::string>(),
            "task id of the function"
    );
    desc.add_options()(
            "libs",
            boost::program_options::value<std::vector<std::string>>(),
            "dynamic libraries that include the spider tasks"
    );
    desc.add_options()(
            "input-pipe",
            boost::program_options::value<int>(),
            "file number of the input pipe"
    );
    desc.add_options()(
            "output-pipe",
            boost::program_options::value<int>(),
            "file number of the output pipe"
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
constexpr int cSignalHandleErr = 2;
constexpr int cStorageErr = 3;
constexpr int cDllErr = 4;
constexpr int cFuncArgParseErr = 5;
constexpr int cResultSendErr = 6;
constexpr int cOtherErr = 7;

auto main(int const argc, char** argv) -> int {
    // Set up spdlog to write to stderr
    // NOLINTNEXTLINE(misc-include-cleaner)
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [spider.executor] %v");
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#endif

    boost::program_options::variables_map const args = parse_arg(argc, argv);

    std::string func_name;
    std::string storage_url;
    std::string task_id_string;
    int input_pipe_fd = -1;
    int output_pipe_fd = -1;
    try {
        if (!args.contains("func")) {
            return cCmdArgParseErr;
        }
        func_name = args["func"].as<std::string>();
        if (!args.contains("task_id")) {
            return cCmdArgParseErr;
        }
        task_id_string = args["task_id"].as<std::string>();
        if (false == args.contains("input-pipe")) {
            return cCmdArgParseErr;
        }
        input_pipe_fd = args["input-pipe"].as<int>();
        if (input_pipe_fd < 0) {
            spdlog::error("Invalid input pipe file descriptor: {}", input_pipe_fd);
            return cCmdArgParseErr;
        }
        if (false == args.contains("output-pipe")) {
            return cCmdArgParseErr;
        }
        output_pipe_fd = args["output-pipe"].as<int>();
        if (output_pipe_fd < 0) {
            spdlog::error("Invalid output pipe file descriptor: {}", output_pipe_fd);
            return cCmdArgParseErr;
        }
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
        // Parse task id
        boost::uuids::string_generator const gen;
        boost::uuids::uuid const task_id = gen(task_id_string);

        // Set up storage
        std::shared_ptr<spider::core::StorageFactory> const storage_factory
                = std::make_shared<spider::core::MySqlStorageFactory>(storage_url);
        std::shared_ptr<spider::core::MetadataStorage> const metadata_store
                = storage_factory->provide_metadata_storage();
        std::shared_ptr<spider::core::DataStorage> const data_store
                = storage_factory->provide_data_storage();

        // Set up asio
        boost::asio::io_context context;
        boost::asio::posix::stream_descriptor in(context, input_pipe_fd);
        boost::asio::posix::stream_descriptor out(context, output_pipe_fd);

        // Get args buffer from stdin
        std::optional<msgpack::sbuffer> request_buffer_option = spider::worker::receive_message(in);
        if (!request_buffer_option.has_value()) {
            spdlog::error("Cannot read args buffer request");
            return cFuncArgParseErr;
        }
        msgpack::sbuffer const& request_buffer = request_buffer_option.value();
        spider::worker::TaskExecutorRequestParser const request_parser{request_buffer};
        if (spider::worker::TaskExecutorRequestType::Arguments != request_parser.get_type()) {
            spdlog::error("Expect args request.");
            return cFuncArgParseErr;
        }
        msgpack::object const args_object = request_parser.get_body();
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
        spider::TaskContext task_context = spider::core::TaskContextImpl::create_task_context(
                task_id,
                data_store,
                metadata_store,
                storage_factory
        );
        msgpack::sbuffer const result_buffer = (*function)(task_context, task_id, args_buffer);
        spdlog::debug("Function executed");

        // Write result buffer to stdout
        spider::worker::send_message(out, result_buffer);
    } catch (std::exception& e) {
        spdlog::error("Exception thrown: {}", e.what());
        return cOtherErr;
    }
    return 0;
}
