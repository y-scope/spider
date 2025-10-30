#ifndef SPIDER_UTILS_LOGGING_HPP
#define SPIDER_UTILS_LOGGING_HPP

#include <string_view>

#include <boost/uuid/uuid.hpp>

namespace spider::utils {
/**
 * Sets up the logger to write to `$SPIDER_LOG_FILE` if the environment variable is set.
 * Writes logs to console otherwise.
 *
 * @param logger_name The name of the logger.
 * @param tag The tag to be included in the log entry.
 */
auto setup_file_logger(std::string_view logger_name, std::string_view tag) -> void;

/**
 * Sets up the logger to write to `$SPIDER_LOG_DIR/<logger_name>_<id>.log` if the environment
 * variable is set.
 * Writes logs to console otherwise.
 *
 * @param logger_name The name of the logger.
 * @param tag The tag to be included in the log entry.
 * @param id The unique identifier to be included in the log file name.
 */
auto
setup_directory_logger(std::string_view logger_name, std::string_view tag, boost::uuids::uuid id)
        -> void;
}  // namespace spider::utils

#endif
