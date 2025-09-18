#ifndef SPIDER_STORAGE_MYSQLSTMT_HPP
#define SPIDER_STORAGE_MYSQLSTMT_HPP

#include <array>
#include <string>

namespace spider::core::mysql {
// NOLINTBEGIN(cert-err58-cpp)

std::string const cCreateDriverTable = R"(CREATE TABLE IF NOT EXISTS `drivers` (
    `id` BINARY(16) NOT NULL,
    `heartbeat` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
))";

std::string const cCreateSchedulerTable = R"(CREATE TABLE IF NOT EXISTS `schedulers` (
    `id` BINARY(16) NOT NULL,
    `address` VARCHAR(40) NOT NULL,
    `port` INT UNSIGNED NOT NULL,
    CONSTRAINT `scheduler_driver_id` FOREIGN KEY (`id`) REFERENCES `drivers` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
))";

std::string const cCreateJobTable = R"(CREATE TABLE IF NOT EXISTS jobs (
    `id` BINARY(16) NOT NULL,
    `client_id` BINARY(16) NOT NULL,
    `creation_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `state` ENUM('running', 'success', 'cancel', 'fail') NOT NULL DEFAULT 'running',
    KEY (`client_id`) USING BTREE,
    INDEX idx_jobs_creation_time (`creation_time`),
    INDEX idx_jobs_state (`state`),
    PRIMARY KEY (`id`)
))";

std::string const cCreateTaskTable = R"(CREATE TABLE IF NOT EXISTS tasks (
    `id` BINARY(16) NOT NULL,
    `job_id` BINARY(16) NOT NULL,
    `func_name` VARCHAR(64) NOT NULL,
    `language` ENUM('cpp', 'python') NOT NULL,
    `state` ENUM('pending', 'ready', 'running', 'success', 'cancel', 'fail') NOT NULL,
    `timeout` FLOAT,
    `max_retry` INT UNSIGNED DEFAULT 0,
    `retry` INT UNSIGNED DEFAULT 0,
    `instance_id` BINARY(16),
    CONSTRAINT `task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`state`),
    INDEX (`func_name`),
    PRIMARY KEY (`id`)
))";

std::string const cCreateInputTaskTable = R"(CREATE TABLE IF NOT EXISTS input_tasks (
    `job_id` BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    CONSTRAINT `input_task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `input_task_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`job_id`, `position`),
    PRIMARY KEY (`task_id`)
))";

std::string const cCreateOutputTaskTable = R"(CREATE TABLE IF NOT EXISTS output_tasks (
    `job_id` BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    CONSTRAINT `output_task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `output_task_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`job_id`, `position`),
    PRIMARY KEY (`task_id`)
))";

std::string const cCreateTaskInputTable = R"(CREATE TABLE IF NOT EXISTS `task_inputs` (
    `task_id` BINARY(16) NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    `type` VARCHAR(999) NOT NULL,
    `output_task_id` BINARY(16),
    `output_task_position` INT UNSIGNED,
    `value` VARBINARY(999), -- Use VARBINARY for all types of values
    `data_id` BINARY(16),
    CONSTRAINT `input_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `input_task_output_match` FOREIGN KEY (`output_task_id`, `output_task_position`) REFERENCES task_outputs (`task_id`, `position`) ON UPDATE NO ACTION ON DELETE SET NULL,
    CONSTRAINT `input_data_id` FOREIGN KEY (`data_id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION,
    PRIMARY KEY (`task_id`, `position`)
))";

std::string const cCreateTaskOutputTable = R"(CREATE TABLE IF NOT EXISTS `task_outputs` (
    `task_id` BINARY(16) NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    `type` VARCHAR(999) NOT NULL,
    `value` VARBINARY(999),
    `data_id` BINARY(16),
    CONSTRAINT `output_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `output_data_id` FOREIGN KEY (`data_id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION,
    PRIMARY KEY (`task_id`, `position`)
))";

std::string const cCreateTaskDependencyTable = R"(CREATE TABLE IF NOT EXISTS `task_dependencies` (
    `parent` BINARY(16) NOT NULL,
    `child` BINARY(16) NOT NULL,
    KEY (`parent`) USING BTREE,
    KEY (`child`) USING BTREE,
    CONSTRAINT `task_dep_parent` FOREIGN KEY (`parent`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `task_dep_child` FOREIGN KEY (`child`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

std::string const cCreateTaskInstanceTable = R"(CREATE TABLE IF NOT EXISTS `task_instances` (
    `id` BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    `start_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT `instance_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
))";

std::string const cCreateSchedulerLeaseTable = R"(CREATE TABLE IF NOT EXISTS `scheduler_leases` (
    `scheduler_id` BINARY(16) NOT NULL,
    `task_id`      BINARY(16) NOT NULL,
    `lease_time`   TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT `lease_scheduler_id` FOREIGN KEY (`scheduler_id`) REFERENCES `schedulers` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `lease_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`scheduler_id`),
    PRIMARY KEY (`scheduler_id`, `task_id`)
))";

std::string const cCreateDataTable = R"(CREATE TABLE IF NOT EXISTS `data` (
    `id` BINARY(16) NOT NULL,
    `value` VARBINARY(999) NOT NULL,
    `hard_locality` BOOL DEFAULT FALSE,
    `persisted` BOOL DEFAULT FALSE,
    PRIMARY KEY (`id`)
))";

std::string const cCreateDataLocalityTable = R"(CREATE TABLE IF NOT EXISTS `data_locality` (
    `id` BINARY(16) NOT NULL,
    `address` VARCHAR(40) NOT NULL,
    KEY (`id`) USING BTREE,
    CONSTRAINT `locality_data_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

std::string const cCreateDataRefDriverTable = R"(CREATE TABLE IF NOT EXISTS `data_ref_driver` (
    `id` BINARY(16) NOT NULL,
    `driver_id` BINARY(16) NOT NULL,
    KEY (`id`) USING BTREE,
    KEY (`driver_id`) USING BTREE,
    CONSTRAINT `data_driver_ref_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `data_ref_driver_id` FOREIGN KEY (`driver_id`) REFERENCES `drivers` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

std::string const cCreateDataRefTaskTable = R"(CREATE TABLE IF NOT EXISTS `data_ref_task` (
    `id` BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    KEY (`id`) USING BTREE,
    KEY (`task_id`) USING BTREE,
    CONSTRAINT `data_task_ref_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `data_ref_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

std::string const cCreateClientKVDataTable = R"(CREATE TABLE IF NOT EXISTS `client_kv_data` (
    `kv_key` VARCHAR(64) NOT NULL,
    `value` VARBINARY(999) NOT NULL,
    `client_id` BINARY(16) NOT NULL,
    PRIMARY KEY (`client_id`, `kv_key`)
))";

std::string const cCreateTaskKVDataTable = R"(CREATE TABLE IF NOT EXISTS `task_kv_data` (
    `kv_key` VARCHAR(64) NOT NULL,
    `value` VARBINARY(999) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    PRIMARY KEY (`task_id`, `kv_key`),
    CONSTRAINT `kv_data_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
))";

std::array<std::string const, 17> const cCreateStorage = {
        cCreateDriverTable,  // drivers table must be created before data_ref_driver
        cCreateSchedulerTable,
        cCreateJobTable,  // jobs table must be created before task
        cCreateTaskTable,  // tasks table must be created before data_ref_task
        cCreateDataTable,  // data table must be created before task_outputs
        cCreateDataLocalityTable,
        cCreateDataRefDriverTable,
        cCreateDataRefTaskTable,
        cCreateClientKVDataTable,
        cCreateTaskKVDataTable,
        cCreateInputTaskTable,
        cCreateOutputTaskTable,
        cCreateTaskOutputTable,  // task_outputs table must be created before task_inputs
        cCreateTaskInputTable,
        cCreateTaskDependencyTable,
        cCreateTaskInstanceTable,
        cCreateSchedulerLeaseTable  // scheduler_lease table must be created after scheduler and
                                    // task
};

std::string const cInsertJob = R"(INSERT INTO `jobs` (`id`, `client_id`) VALUES (?, ?))";

std::string const cInsertTask
        = R"(INSERT INTO `tasks` (`id`, `job_id`, `func_name`, `language`, `state`, `timeout`, `max_retry`) VALUES (?, ?, ?, ?, ?, ?, ?))";

std::string const cInsertTaskInputOutput
        = R"(INSERT INTO `task_inputs` (`task_id`, `position`, `type`, `output_task_id`, `output_task_position`) VALUES (?, ?, ?, ?, ?))";

std::string const cInsertTaskInputData
        = R"(INSERT INTO `task_inputs` (`task_id`, `position`, `type`, `data_id`) VALUES (?, ?, ?, ?))";

std::string const cInsertTaskInputValue
        = R"(INSERT INTO `task_inputs` (`task_id`, `position`, `type`, `value`) VALUES (?, ?, ?, ?))";

std::string const cInsertTaskOutput
        = R"(INSERT INTO `task_outputs` (`task_id`, `position`, `type`) VALUES (?, ?, ?))";

std::string const cInsertTaskDependency
        = R"(INSERT INTO `task_dependencies` (parent, child) VALUES (?, ?))";

std::string const cInsertInputTask
        = R"(INSERT INTO `input_tasks` (`job_id`, `task_id`, `position`) VALUES (?, ?, ?))";

std::string const cInsertOutputTask
        = R"(INSERT INTO `output_tasks` (`job_id`, `task_id`, `position`) VALUES (?, ?, ?))";

// NOLINTEND(cert-err58-cpp)
}  // namespace spider::core::mysql

#endif
