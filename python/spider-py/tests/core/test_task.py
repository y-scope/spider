"""Test core task."""

from spider_py.core import Task


def test_task() -> None:
    """Tests task created is not None."""
    task = Task()
    assert task is not None
