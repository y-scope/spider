CREATE TABLE IF NOT EXISTS `drivers`
(
    `id`        BINARY(16) NOT NULL,
    `heartbeat` TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
);
CREATE TABLE IF NOT EXISTS `schedulers`
(
    `id`      BINARY(16)                        NOT NULL,
    `address` VARCHAR(40)                       NOT NULL,
    `port`    INT UNSIGNED                      NOT NULL,
    CONSTRAINT `scheduler_driver_id` FOREIGN KEY (`id`) REFERENCES `drivers` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
);
CREATE TABLE IF NOT EXISTS jobs
(
    `id`            BINARY(16) NOT NULL,
    `client_id`     BINARY(16) NOT NULL,
    `creation_time` TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `state`         ENUM ('running', 'success', 'fail', 'cancel') NOT NULL DEFAULT 'running',
    KEY (`client_id`) USING BTREE,
    INDEX idx_jobs_creation_time (`creation_time`),
    INDEX idx_jobs_state (`state`),
    PRIMARY KEY (`id`)
);
CREATE TABLE IF NOT EXISTS tasks
(
    `id`          BINARY(16)                                                        NOT NULL,
    `job_id`      BINARY(16)                                                        NOT NULL,
    `func_name`   VARCHAR(64)                                                       NOT NULL,
    `language`    ENUM('cpp', 'python')                                             NOT NULL,
    `state`       ENUM ('pending', 'ready', 'running', 'success', 'cancel', 'fail') NOT NULL,
    `timeout`     FLOAT,
    `max_retry`   INT UNSIGNED DEFAULT 0,
    `retry`       INT UNSIGNED DEFAULT 0,
    `instance_id` BINARY(16),
    CONSTRAINT `task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
);
CREATE TABLE IF NOT EXISTS input_tasks
(
    `job_id`   BINARY(16)   NOT NULL,
    `task_id`  BINARY(16)   NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    CONSTRAINT `input_task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `input_task_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`job_id`, `position`),
    PRIMARY KEY (`task_id`)
);
CREATE TABLE IF NOT EXISTS output_tasks
(
    `job_id`   BINARY(16)   NOT NULL,
    `task_id`  BINARY(16)   NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    CONSTRAINT `output_task_job_id` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `output_task_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`job_id`, `position`),
    PRIMARY KEY (`task_id`)
);
CREATE TABLE IF NOT EXISTS `data`
(
    `id`            BINARY(16)     NOT NULL,
    `value`         VARBINARY(999) NOT NULL,
    `hard_locality` BOOL DEFAULT FALSE,
    `persisted`     BOOL DEFAULT FALSE,
    PRIMARY KEY (`id`)
);
CREATE TABLE IF NOT EXISTS `task_outputs`
(
    `task_id`  BINARY(16)   NOT NULL,
    `position` INT UNSIGNED NOT NULL,
    `type`     VARCHAR(999)  NOT NULL,
    `value`    VARBINARY(999),
    `data_id`  BINARY(16),
    CONSTRAINT `output_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `output_data_id` FOREIGN KEY (`data_id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION,
    PRIMARY KEY (`task_id`, `position`)
);
CREATE TABLE IF NOT EXISTS `task_inputs`
(
    `task_id`              BINARY(16)   NOT NULL,
    `position`             INT UNSIGNED NOT NULL,
    `type`                 VARCHAR(999)  NOT NULL,
    `output_task_id`       BINARY(16),
    `output_task_position` INT UNSIGNED,
    `value`                VARBINARY(999), -- Use VARBINARY for all types of values
    `data_id`              BINARY(16),
    CONSTRAINT `input_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `input_task_output_match` FOREIGN KEY (`output_task_id`, `output_task_position`) REFERENCES task_outputs (`task_id`, `position`) ON UPDATE NO ACTION ON DELETE SET NULL,
    CONSTRAINT `input_data_id` FOREIGN KEY (`data_id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION,
    PRIMARY KEY (`task_id`, `position`)
);

CREATE TABLE IF NOT EXISTS `task_dependencies`
(
    `parent` BINARY(16) NOT NULL,
    `child`  BINARY(16) NOT NULL,
    KEY (`parent`) USING BTREE,
    KEY (`child`) USING BTREE,
    CONSTRAINT `task_dep_parent` FOREIGN KEY (`parent`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `task_dep_child` FOREIGN KEY (`child`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS `task_instances`
(
    `id`         BINARY(16) NOT NULL,
    `task_id`    BINARY(16) NOT NULL,
    `start_time` TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT `instance_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    PRIMARY KEY (`id`)
);
CREATE TABLE IF NOT EXISTS `scheduler_leases`
(
    `scheduler_id` BINARY(16) NOT NULL,
    `task_id`      BINARY(16) NOT NULL,
    `lease_time`   TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT `lease_scheduler_id` FOREIGN KEY (`scheduler_id`) REFERENCES `schedulers` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `lease_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    INDEX (`scheduler_id`),
    PRIMARY KEY (`scheduler_id`, `task_id`)
);
CREATE TABLE IF NOT EXISTS `data_locality`
(
    `id`      BINARY(16)  NOT NULL,
    `address` VARCHAR(40) NOT NULL,
    KEY (`id`) USING BTREE,
    CONSTRAINT `locality_data_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS `data_ref_driver`
(
    `id`        BINARY(16) NOT NULL,
    `driver_id` BINARY(16) NOT NULL,
    KEY (`id`) USING BTREE,
    KEY (`driver_id`) USING BTREE,
    CONSTRAINT `data_driver_ref_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `data_ref_driver_id` FOREIGN KEY (`driver_id`) REFERENCES `drivers` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS `data_ref_task`
(
    `id`      BINARY(16) NOT NULL,
    `task_id` BINARY(16) NOT NULL,
    KEY (`id`) USING BTREE,
    KEY (`task_id`) USING BTREE,
    CONSTRAINT `data_task_ref_id` FOREIGN KEY (`id`) REFERENCES `data` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT `data_ref_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS `client_kv_data`
(
    `kv_key`    VARCHAR(64)    NOT NULL,
    `value`     VARBINARY(999) NOT NULL,
    `client_id` BINARY(16)     NOT NULL,
    PRIMARY KEY (`client_id`, `kv_key`)
);
CREATE TABLE IF NOT EXISTS `task_kv_data`
(
    `kv_key`  VARCHAR(64)    NOT NULL,
    `value`   VARBINARY(999) NOT NULL,
    `task_id` BINARY(16)     NOT NULL,
    PRIMARY KEY (`task_id`, `kv_key`),
    CONSTRAINT `kv_data_task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON UPDATE NO ACTION ON DELETE CASCADE
);