#ifndef SPIDER_UTILS_LOGGING_HPP
#define SPIDER_UTILS_LOGGING_HPP

#include <string>

#include <boost/uuid/uuid.hpp>

namespace spider::logging {
/**
 * Sets up the logger to write to `$SPIDER_LOG_FILE` if the environment variable is set.
 * Writes logs to console otherwise.
 *
 * @param logger_name The name of the logger.
 * @param source_name The source name to be included in the log line.
 */
auto setup_file_logger(std::string const& logger_name, std::string const& source_name) -> void;

/**
 * Sets up the logger to write to `$SPIDER_LOG_DIR/<logger_name>_<id>.log` if the environment
 * variable is set.
 * Writes logs to console otherwise.
 *
 * @param logger_name The name of the logger.
 * @param source_name The source name to be included in the log line.
 * @param id The unique identifier to be included in the log file name.
 */
auto setup_directory_logger(
        std::string const& logger_name,
        std::string const& source_name,
        boost::uuids::uuid id
) -> void;
}  // namespace spider::logging

#endif
