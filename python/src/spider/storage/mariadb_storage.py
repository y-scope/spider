"""MariaDB Storage module."""

from collections.abc import Sequence

import mariadb
from typing_extensions import override

from spider import core
from spider.storage.jdbc_url import JdbcParameters
from spider.storage.storage import Storage, StorageError


class MariaDBStorage(Storage):
    """MairaDB Storage class."""

    def __init__(self, params: JdbcParameters) -> None:
        """
        Connects to the MariaDB database.
        :param params: The JDBC parameters for connecting to the database.
        :raises StorageError: If the connection to the database fails.
        """
        try:
            self._conn = mariadb.connect(**params.__dict__)
        except mariadb.Error as e:
            raise StorageError(str(e)) from e

    @override
    def submit_jobs(self, task_graphs: Sequence[core.TaskGraph]) -> None:
        pass
