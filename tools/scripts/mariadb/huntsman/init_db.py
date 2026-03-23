#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "mariadb>=1.1.14",
# ]
# ///
"""Script to initialize database tables for Spider."""

import argparse
import logging
import sys

import mariadb  # type: ignore [import-not-found]

_TABLE_CREATORS = [
    """
    CREATE TABLE IF NOT EXISTS `resource_groups` (
      `id` UUID NOT NULL DEFAULT UUID_v7(),
      `external_id` VARCHAR(256) NOT NULL,
      `password` VARCHAR(2048) NOT NULL,
      PRIMARY KEY (`id`),
      UNIQUE INDEX `external_resource_group_id` (`external_id`)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS `jobs` (
      `id` UUID NOT NULL DEFAULT UUID_v7(),
      `resource_group_id` UUID NOT NULL,
      `state` ENUM(
        'Ready',
        'Running',
        'CommitReady',
        'CleanupReady',
        'Succeeded',
        'Failed',
        'Cancelled'
      ) NOT NULL DEFAULT 'Ready',
      `serialized_task_graph` LONGTEXT NOT NULL,
      `serialized_job_inputs` LONGTEXT NOT NULL,
      `serialized_job_outputs` LONGTEXT,
      `error_message` LONGTEXT,
      `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      `ended_at` TIMESTAMP,
      `max_num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
      `num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
      PRIMARY KEY (`id`),
      CONSTRAINT `job_resource_group` FOREIGN KEY (`resource_group_id`)
        REFERENCES `resource_groups` (`id`)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    );
    """,
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Main."""
    parser = argparse.ArgumentParser(description="Initialize the database tables for Spider.")
    parser.add_argument(
        "--port",
        type=int,
        default=3306,
        help="The port MariaDB is hosting on (default: %(default)d)",
    )
    parser.add_argument(
        "--username",
        type=str,
        default="spider-user",
        help="The username of the started MariaDB (default: %(default)s)",
    )
    parser.add_argument(
        "--password",
        type=str,
        default="spider-password",
        help="The password of the started MariaDB (default: %(default)s)",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="spider-db",
        help="The database name of the started MariaDB (default: %(default)s)",
    )
    args = parser.parse_args()

    with (
        mariadb.connect(
            host="127.0.0.1",
            port=args.port,
            user=args.username,
            password=args.password,
            database=args.database,
        ) as conn,
        conn.cursor() as cursor,
    ):
        for table_creator in _TABLE_CREATORS:
            cursor.execute(table_creator)
        conn.commit()

    return 0


if __name__ == "__main__":
    sys.exit(main())
