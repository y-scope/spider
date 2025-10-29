#include "logging.hpp"

#include <cstdlib>
#include <filesystem>
#include <string>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>
#include <spdlog/common.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace spider::logging {
namespace {
/**
 * Sets up a file logger with a fallback to console logger if file creation fails.
 * @param logger_name The name of the logger.
 * @param log_file_path The path of the log file.
 */
auto setup_logger(std::string const& logger_name, std::string const& log_file_path) -> void {
    try {
        auto const file_logger = spdlog::basic_logger_mt(logger_name, log_file_path);
        spdlog::set_default_logger(file_logger);
        spdlog::flush_on(spdlog::level::info);
    } catch (spdlog::spdlog_ex& ex) {
        auto const console_logger = spdlog::stdout_color_mt(fmt::format("{}_console", logger_name));
        spdlog::set_default_logger(console_logger);
        spdlog::flush_on(spdlog::level::info);
    }
}

/**
 * Sets up the log format and level for the spdlog logger.
 * @param source_name
 */
auto setup_log_format(std::string const& source_name) -> void {
    // NOLINTNEXTLINE(misc-include-cleaner)
    spdlog::set_pattern(fmt::format("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [{}] %v", source_name));
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#endif
}
}  // namespace

auto setup_file_logger(std::string const& logger_name, std::string const& source_name) -> void {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char const* const log_file_path = std::getenv("SPIDER_LOG_FILE");
    if (nullptr == log_file_path) {
        setup_log_format(source_name);
        return;
    }

    setup_logger(logger_name, log_file_path);

    setup_log_format(source_name);
}

auto setup_directory_logger(
        std::string const& logger_name,
        std::string const& source_name,
        boost::uuids::uuid id
) -> void {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char const* const log_file_dir = std::getenv("SPIDER_LOG_DIR");
    if (nullptr == log_file_dir) {
        setup_log_format(source_name);
        return;
    }

    auto const log_file_path = std::filesystem::path{log_file_dir}
                               / fmt::format("{}_{}.log", logger_name, to_string(id));

    setup_logger(logger_name, log_file_path.string());

    setup_log_format(source_name);
}
}  // namespace spider::logging
