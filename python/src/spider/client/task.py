"""Spider client task module."""

import inspect
from collections.abc import Callable
from types import FunctionType, GenericAlias
from typing import get_args, get_origin

from spider import core
from spider.client.data import Data
from spider.core import TaskInput, TaskOutput, TaskOutputValue, TaskOutputData
from spider.type import to_tdl_type_str


class TaskContext:
    """Spider task context."""

    # TODO: Implement task context for use in task executor


# Check the TaskFunction signature at runtime.
# Enforcing static check for first argument requires the use of Protocol. However, functions, which
# are Callable, are not considered a Protocol without explicit cast.
TaskFunction = Callable[..., object]


def is_tuple(t: type | GenericAlias) -> bool:
    """
    :param t:
    :return: Whether t is a tuple.
    """
    return get_origin(t) is tuple


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
    if not params or params[0].annotation is not TaskContext:
        msg = "First argument is not a TaskContext."
        raise TypeError(msg)
    for param in params[1:]:
        if param.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            raise TypeError("Variadic parameters are not supported in task functions.")
        if param.annotation == inspect.Parameter.empty:
            msg = "Argument must have type annotation"
            raise TypeError(msg)
        tdl_type_str = to_tdl_type_str(param.annotation)
        task.task_inputs.append(TaskInput(tdl_type_str, None))
    returns = signature.return_annotation
    if returns == inspect.Parameter.empty:
        msg = "Return type must have type annotation"
        raise TypeError(msg)
    if is_tuple(returns):
        for r in get_args(returns):
            tdl_type_str = to_tdl_type_str(r)
            if r is Data:
                task.task_outputs.append(TaskOutput(tdl_type_str, TaskOutputData()))
            else:
                task.task_outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))
    else:
        tdl_type_str = to_tdl_type_str(returns)
        if returns is Data:
            task.task_outputs.append(TaskOutput(tdl_type_str, TaskOutputData()))
        else:
            task.task_outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))

    return task
