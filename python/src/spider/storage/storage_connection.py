"""Storage Connection Interface module."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from types import TracebackType


class StorageError(Exception):
    """Storage Exception."""

    def __init__(self, msg: str) -> None:
        """Creates a storage exception."""
        super().__init__(msg)


class StorageCursor(ABC):
    """Abstract base class for storage cursor."""

    @abstractmethod
    def execute(self, query: str, params: tuple[object]) -> None:
        """
        Executes a query on the storage cursor.
        To get the result, call fetch* functions.
        :param query: The query to execute.
        :param params: The query parameters.
        :raise StorageError: If query execution fails.
        """

    @abstractmethod
    def executemany(self, query: str, params: Sequence[tuple[object]]) -> None:
        """
        Executes a batch query on the storage cursor.
        :param query: The query to execute.
        :param params: Batch of query parameters.
        :raise StorageError: If query execution fails.
        """

    @abstractmethod
    def fetchone(self) -> tuple[object]:
        """
        :return: The next result from the previous query.
        :raise StorageError: If query execution fails.
        """

    @abstractmethod
    def fetchmany(self, size: int) -> Sequence[tuple[object]]:
        """
        :param size: The number of results to fetch.
        :return: The results from the previous query. At most `size` results are returned.
        :raise StorageError: If query execution fails.
        """

    @abstractmethod
    def fetchall(self) -> Sequence[tuple[object]]:
        """
        :return: All results from the previous query.
        :raise StorageError: If query execution fails.
        """

    @abstractmethod
    def open(self) -> None:
        """Opens the storage cursor."""

    @abstractmethod
    def close(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """
        Closes the storage cursor.
        :param exc_type: The exception type.
        :param exc_value: The exception object.
        :param traceback: The exception traceback.
        :return: Whether the exception is suppressed.
        """

    def __enter__(self) -> "StorageCursor":
        """Opens the storage cursor."""
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """
        Closes the storage cursor.
        :param exc_type: The exception type.
        :param exc_value: The exception object.
        :param traceback: The exception traceback.
        :return: Whether the exception is suppressed.
        """
        return self.close(exc_type, exc_value, traceback)
