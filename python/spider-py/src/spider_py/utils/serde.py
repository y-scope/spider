"""Serialization and deserialization using msgpack."""

from dataclasses import fields, is_dataclass
from types import GenericAlias
from typing import cast, get_args, get_origin


def to_serializable(obj: object) -> object:
    """
    Transforms an object into serializable form that comprises only built-in primitive types and
    collections (dicts and lists).
    If `obj` is a build-in container type (list or dict), its elements are recursively transformed.
    If `obj` is a dataclass, it is transformed into a dict mapping field names to values.
    Otherwise, `obj` is returned as-is.
    :param obj:
    :return: A serializable representation of `obj`.
    """
    if is_dataclass(obj):
        return {field.name: to_serializable(getattr(obj, field.name)) for field in fields(obj)}
    if isinstance(obj, list):
        return [to_serializable(item) for item in obj]
    if isinstance(obj, dict):
        return {to_serializable(key): to_serializable(val) for key, val in obj.items()}
    return obj


def from_serializable(cls: type | GenericAlias, data: object) -> object:
    """
    Transforms data in serializable form back into an instance of `cls`.
    :param cls: Class to deserialize into.
    :param data: Data in serializable form.
    :return: Transformed instance of `cls` from `data`.
    :raise: TypeError if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create instance of {cls} with {data!r}."

    origin = get_origin(cls)
    if origin is None:
        # If `cls` does not have an origin, it is a concrete type.
        return _deserialize_as_class(cast("type", cls), data)

    if origin is list:
        (key_type,) = get_args(cls)
        if not isinstance(data, list):
            raise TypeError(msg)
        return [from_serializable(key_type, item) for item in data]

    if origin is dict:
        key_type, value_type = get_args(cls)
        if not isinstance(data, dict):
            raise TypeError(msg)
        return {
            from_serializable(key_type, key): from_serializable(value_type, value)
            for key, value in data.items()
        }

    raise TypeError(msg)


def _deserialize_as_class(cls: type, data: object) -> object:
    """
    Deserializes the input data as a `cls` instance.
    :param cls: Class to deserialize into. Must not be a container type.
    :param data: Serialized data.
    :return: Transformed instance of `cls` from `data`.
    :raise: TypeError if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create instance of {cls} with {data!r}."
    if not is_dataclass(cls):
        # Fall back to normal constructor if `cls` is not a dataclass.
        return cls(data)

    if not isinstance(data, dict):
        raise TypeError(msg)
    args = {}
    expected_fields = {field.name: field for field in fields(cls)}
    for name, value in data.items():
        if name not in expected_fields:
            raise TypeError(msg)
        expected_field_type = expected_fields[name].type
        if not isinstance(expected_field_type, (type, GenericAlias)):
            raise TypeError(msg)
        args[name] = from_serializable(expected_field_type, value)
    return cls(**args)
