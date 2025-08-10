"""Spider client task module."""

import inspect
from types import FunctionType
from typing import Protocol, runtime_checkable

from spider import core
from spider.client.data import Data
from spider.core import TaskInput, TaskOutput, TaskOutputValue
from spider.type import to_tdl_type_str


class TaskContext:
    """Spider task context."""

    # TODO: Implement task context for use in task executor


@runtime_checkable
class TaskFunction(Protocol):
    """Task function must accept a TaskContext as its first argument."""

    def __call__(self, context: TaskContext, *args: object) -> object:
        """Task function must accept TaskContext as its first argument."""
        ...


def create_task(func: TaskFunction) -> core.Task:
    """
    Creates a core Task object from the task function.
    :param func:
    :return:
    :raise TypeError: If the function signature contains unsupported types.
    """
    task = core.Task()
    if not isinstance(func, FunctionType):
        msg = "`func` is not a function."
        raise TypeError(msg)
    task.function_name = func.__qualname__
    signature = inspect.signature(func)
    params = list(signature.parameters.values())
    if params[0].annotation is not TaskContext:
        msg = "First argument is not a TaskContext."
        raise TypeError(msg)
    for param in params[1:]:
        if param.annotation == inspect.Parameter.empty:
            msg = "Argument must has type annotation"
            raise TypeError(msg)
        tdl_type_str = to_tdl_type_str(param.annotation)
        task.task_inputs.append(TaskInput(tdl_type_str, None))
    returns = signature.return_annotation
    if returns == inspect.Parameter.empty:
        msg = "Return type must has type annotation"
        raise TypeError(msg)
    if type(returns) is tuple:
        for r in returns:
            tdl_type_str = to_tdl_type_str(r)
            if r is Data:
                task.task_outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))
            else:
                task.task_outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))
    else:
        tdl_type_str = to_tdl_type_str(returns)
        if returns is Data:
            task.task_outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))
        else:
            task.task_outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))

    return task
