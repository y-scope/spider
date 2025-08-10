"""Spider client task module."""

from typing import ParamSpec, Protocol, TypeVar


class TaskContext:
    """Spider task context."""

    # TODO: Implement task context for use in task executor


P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)


class TaskFunction(Protocol[P, R_co]):
    """Task function accepts TaskContext as its first argument."""

    def __call__(self, context: TaskContext, *args: P.args, **kwargs: P.kwargs) -> R_co:
        """Task function accepts TaskContext as its first argument."""
        ...
