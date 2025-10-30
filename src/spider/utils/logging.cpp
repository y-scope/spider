#include "logging.hpp"

#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>
#include <spdlog/common.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace spider::utils {
namespace {
/**
 * Sets up a file logger with a fallback to console logger if file creation fails.
 * @param logger_name The name of the logger.
 * @param log_file_path The path of the log file.
 */
auto set_default_logger(std::string_view logger_name, std::string_view log_file_path) -> void;

/**
 * Sets up the log format and level for the spdlog logger.
 * @param tag The tag to be included in the log entry.
 */
auto set_log_config(std::string_view tag) -> void;

auto set_default_logger(std::string_view const logger_name, std::string_view const log_file_path)
        -> void {
    try {
        auto const file_logger
                = spdlog::basic_logger_mt(std::string{logger_name}, std::string{log_file_path});
        spdlog::set_default_logger(file_logger);
    } catch (spdlog::spdlog_ex& ex) {
        auto const console_logger = spdlog::stdout_color_mt(fmt::format("{}_console", logger_name));
        spdlog::set_default_logger(console_logger);
    }
}

auto set_log_config(std::string_view const tag) -> void {
    // NOLINTNEXTLINE(misc-include-cleaner)
    spdlog::set_pattern(fmt::format("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [{}] %v", tag));
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
    spdlog::flush_on(spdlog::level::trace);
#else
    spdlog::set_level(spdlog::level::warn);
    spdlog::flush_on(spdlog::level::warn);
#endif
}
}  // namespace

auto setup_file_logger(std::string_view const logger_name, std::string_view const tag) -> void {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char const* const log_file_path = std::getenv("SPIDER_LOG_FILE");
    if (nullptr == log_file_path) {
        set_log_config(tag);
        return;
    }

    set_default_logger(logger_name, log_file_path);

    set_log_config(tag);
}

auto setup_directory_logger(
        std::string_view const logger_name,
        std::string_view const tag,
        boost::uuids::uuid const id
) -> void {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char const* const log_file_dir = std::getenv("SPIDER_LOG_DIR");
    if (nullptr == log_file_dir) {
        set_log_config(tag);
        return;
    }

    auto const log_file_path = std::filesystem::path{log_file_dir}
                               / fmt::format("{}_{}.log", logger_name, to_string(id));

    set_default_logger(logger_name, log_file_path.string());

    set_log_config(tag);
}
}  // namespace spider::utils
