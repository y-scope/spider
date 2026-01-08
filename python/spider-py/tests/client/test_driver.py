"""Tests for the driver module."""

import os
from dataclasses import dataclass

import pytest

from spider_py import chain, Driver, group, Int8, Int64, TaskContext

MariaDBTestUrl = "jdbc:mariadb://127.0.0.1:3306/spider-storage?user=spider&password=password"


@pytest.fixture(scope="session")
def driver() -> Driver:
    """Fixture for the driver."""
    url = os.getenv("SPIDER_STORAGE_URL", MariaDBTestUrl)
    return Driver(url)


def double(_: TaskContext, x: Int8) -> Int8:
    """Double a number."""
    return Int8(x * 2)


def swap(_: TaskContext, x: Int8, y: Int8) -> tuple[Int8, Int8]:
    """Swaps two numbers."""
    return y, x


def count(_: TaskContext, arr: list[Int8]) -> Int64:
    """Counts the number of elements in an array."""
    return Int64(len(arr))


def count_map(_: TaskContext, dictionary: dict[Int8, list[Int8]]) -> Int64:
    """Counts the number of elements in a map."""
    return Int64(len(dictionary))


@dataclass
class User:
    """A user class for testing."""

    id: Int8
    name: list[Int8]


def count_users(_: TaskContext, users: list[User]) -> Int64:
    """Counts the number of users."""
    return Int64(len(users))


@pytest.mark.storage
class TestDriver:
    """Test class for the driver module."""

    def test_job_submission(self, driver: Driver) -> None:
        """Tests successful job submission."""
        jobs = driver.submit_jobs(
            [
                group([double]),
                group([double, double]),
                chain(group([double, double]), swap),
            ],
            [
                (Int8(1),),
                (Int8(1), Int8(2)),
                (Int8(1), Int8(2)),
            ],
        )
        assert len(jobs) == 3

    def test_submit_same_graph(self, driver: Driver) -> None:
        """Tests successful job submission for same graph."""
        graph = group([double])
        jobs = driver.submit_jobs(
            [
                graph,
                graph,
            ],
            [
                (Int8(1),),
                (Int8(1),),
            ],
        )
        assert len(jobs) == 2

    def test_submit_list(self, driver: Driver) -> None:
        """Tests successful job submission for list input."""
        jobs = driver.submit_jobs(
            [
                group([count]),
                group([count]),
            ],
            [([Int8(1), Int8(2), Int8(3)],), ([],)],
        )
        assert len(jobs) == 2

    def test_submit_map(self, driver: Driver) -> None:
        """Tests successful job submission for map input."""
        jobs = driver.submit_jobs(
            [
                group([count_map]),
                group([count_map]),
            ],
            [({Int8(1): [Int8(3), Int8(4)], Int8(5): [Int8(6)]},), ({},)],
        )
        assert len(jobs) == 2

    def test_submit_dataclass(self, driver: Driver) -> None:
        """Tests successful job submission for dataclass input."""
        jobs = driver.submit_jobs(
            [
                group([count_users]),
                group([count_users]),
            ],
            [
                ([User(Int8(1), [Int8(1), Int8(2)]), User(Int8(2), [Int8(3)])],),
                ([],),
            ],
        )
        assert len(jobs) == 2

    def test_job_submission_fail(self, driver: Driver) -> None:
        """Tests job submission failure."""
        with pytest.raises(
            ValueError, match="Number of job inputs does not match number of arguments"
        ):
            driver.submit_jobs(
                [
                    group([double]),
                ],
                [
                    (Int8(1), Int8(2)),
                ],
            )
        with pytest.raises(TypeError):
            # Input type unsupported (non TDL type)
            driver.submit_jobs(
                [
                    group([double]),
                ],
                [
                    (1,),
                ],
            )
        with pytest.raises(TypeError):
            # Input type mismatch
            driver.submit_jobs(
                [
                    group([count_users]),
                ],
                [
                    (["a"],),
                ],
            )
