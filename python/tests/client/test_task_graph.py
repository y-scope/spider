"""Unit tests for Spider client TaskGraph"""
import pytest

from spider import TaskContext, Int8, group, chain

def no_context(x: Int8, y: Int8) -> Int8:
    """Invalid task function with no context."""
    return x + y

def invalid_type(_: TaskContext, x: int) -> int:
    return x + x

def add(_: TaskContext, x: Int8, y: Int8) -> Int8:
    """Adds two numbers."""
    return x + y

def swap(_: TaskContext, x: Int8, y: Int8) -> tuple[Int8, Int8]:
    """Swaps two numbers."""
    return y, x

class TestTaskGraph:

    def test_group(self) -> None:
        """Tests task grouping."""
        graph = group([add, add])
        graph = group([graph, graph])
        assert len(graph._impl.tasks) == 4
        assert len(graph._impl.dependencies) == 0
        assert len(graph._impl.input_tasks) == 4
        assert len(graph._impl.output_tasks) == 4

    def test_task_fail(self) -> None:
        """Tests task failing."""
        with pytest.raises(TypeError):
            group([no_context])
        with pytest.raises(TypeError):
            group([invalid_type])

    def test_chain(self) -> None:
        """Tests task chaining."""
        graph = group([add, add])
        graph = chain(graph, swap)
        assert len(graph._impl.tasks) == 3
        assert len(graph._impl.dependencies) == 2
        assert len(graph._impl.input_tasks) == 2
        assert len(graph._impl.output_tasks) == 1

    def test_chain_fail(self) -> None:
        """Tests task chaining failure."""
        with pytest.raises(TypeError):
            chain([add, add])
