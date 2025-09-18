"""Spider client task module."""

import inspect
from collections.abc import Callable
from types import FunctionType, GenericAlias
from typing import get_args, get_origin

from spider_py import core
from spider_py.client.data import Data
from spider_py.client.task_context import TaskContext
from spider_py.core import DataId, TaskInput, TaskOutput, TaskOutputValue
from spider_py.type import to_tdl_type_str

# NOTE: This type alias is for clarification purposes only. It does not enforce static type checks.
# Instead, we rely on the runtime check to ensure the first argument is `TaskContext`. To statically
# enforce the first argument to be `TaskContext`, `Protocol` is required, which is not compatible
# with `Callable` without explicit type casting.
TaskFunction = Callable[..., object]


def _is_tuple(t: type | GenericAlias) -> bool:
    """
    :param t:
    :return: Whether t is a tuple.
    """
    return get_origin(t) is tuple


def _validate_and_convert_params(signature: inspect.Signature) -> list[TaskInput]:
    """
    Validates the task parameters and converts them into a list of `core.TaskInput`.
    :param signature:
    :return: The converted task parameters.
    :raises TypeError: If the parameters are invalid.
    """
    params = list(signature.parameters.values())
    inputs = []
    if not params or params[0].annotation is not TaskContext:
        msg = "First argument is not a TaskContext."
        raise TypeError(msg)
    for param in params[1:]:
        if param.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            msg = "Variadic parameters are not supported."
            raise TypeError(msg)
        if param.annotation is inspect.Parameter.empty:
            msg = "Parameters must have type annotation."
            raise TypeError(msg)
        tdl_type_str = to_tdl_type_str(param.annotation)
        inputs.append(TaskInput(tdl_type_str, None))
    return inputs


def _validate_and_convert_return(signature: inspect.Signature) -> list[TaskOutput]:
    """
    Validates the task returns and converts them into a list of `core.TaskOutput`.
    :param signature:
    :return: The converted task returns.
    :raises TypeError: If the return type is invalid.
    """
    returns = signature.return_annotation
    outputs = []
    if returns is inspect.Parameter.empty:
        msg = "Return type must have type annotation."
        raise TypeError(msg)

    if not _is_tuple(returns):
        tdl_type_str = to_tdl_type_str(returns)
        if returns is Data:
            outputs.append(TaskOutput(tdl_type_str, DataId(int=0)))
        else:
            outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))
        return outputs

    args = get_args(returns)
    if Ellipsis in args:
        msg = "Variable-length tuple return types are not supported."
        raise TypeError(msg)
    for arg in args:
        tdl_type_str = to_tdl_type_str(arg)
        if arg is Data:
            outputs.append(TaskOutput(tdl_type_str, DataId(int=0)))
        else:
            outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))
    return outputs


def create_task(func: TaskFunction) -> core.Task:
    """
    Creates a core Task object from the task function.
    :param func:
    :return: The created core Task object.
    :raise TypeError: If the function signature contains unsupported types.
    """
    if not isinstance(func, FunctionType):
        msg = "`func` is not a function."
        raise TypeError(msg)
    signature = inspect.signature(func)
    return core.Task(
        function_name=f"{func.__module__}.{func.__qualname__}",
        task_inputs=_validate_and_convert_params(signature),
        task_outputs=_validate_and_convert_return(signature),
    )
