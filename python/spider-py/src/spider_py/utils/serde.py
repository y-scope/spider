"""Serialization and deserialization into serializable format."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from types import GenericAlias
from typing import cast, get_args, get_origin


def to_serializable(obj: object) -> object:
    """
    Converts an object into a serializable format consisting only of built-in primitive types and
    collections (lists and dictionaries).

    - Built-in container types (lists or dictionaries) are recursively transformed.
    - Dataclass instances are converted into dictionaries mapping field names to their serialized
      values.
    - All other objects are returned as-is.

    :param obj: Object to serialize. Must be of types supported by Spider TDL.
    :return: A serializable representation of `obj`.
    """
    if is_dataclass(obj):
        return {field.name: to_serializable(getattr(obj, field.name)) for field in fields(obj)}
    if isinstance(obj, list):
        return [to_serializable(item) for item in obj]
    if isinstance(obj, dict):
        return {to_serializable(key): to_serializable(val) for key, val in obj.items()}
    return obj


def to_serializable_type(obj: object, cls: type | GenericAlias) -> object:
    """
    Converts an object into a serializable format consisting only of built-in primitive types and
    collections (lists and dictionaries), ensuring that the `obj` is of the specified `cls` type.

    :param obj: Object to serialize. Must be of types supported by Spider TDL.
    :param cls: Class to ensure the object is of. Must be a concrete type or GenericAlias supported
    by Spider TDL.
    :return: A serializable representation of `obj` if it matches `cls`.
    :raise: TypeError if `obj` type does not match `cls` or `cls` is not a TDL supported type.
    """
    origin = get_origin(cls)
    if origin is None:
        return _to_serializable_type(obj, cast("type", cls))

    if origin is list:
        return _to_serializable_list(obj, cast("GenericAlias", cls))

    if origin is dict:
        return _to_serializable_dict(obj, cast("GenericAlias", cls))

    msg = f"Unsupported type: {cls!r}"
    raise TypeError(msg)


def from_serializable(cls: type | GenericAlias, data: object) -> object:
    """
    Deserializes data in a serializable format back into an instance of the specified `cls`.

    :param cls: Class to deserialize into. Must be a concrete type, list, or dict.
    :param data: Data in serializable form.
    :return: An instance of `cls` reconstructed from the serialized data.
    :raise: TypeError if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create an instance of {cls} with {data!r}."

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
    :param cls: Class to deserialize into. Must be a non-container type.
    :param data: Serialized data.
    :return: An instance of `cls` reconstructed from the serialized data.
    :raise: TypeError if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create an instance of {cls} with {data!r}."
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


def _to_serializable_type(obj: object, cls: type) -> object:
    """
    Converts an object to a serializable format if it matches the specified concrete type.

    :param obj: Object to serialize. Must be of types supported by Spider TDL.
    :param cls: Class to ensure the object is of. Must be a concrete type.
    :return: A serializable representation of `obj`.
    :raise: TypeError if `obj` type does not match `cls`.
    """
    if not isinstance(obj, cls):
        msg = f"Object {obj!r} is not of type {cls!r}"
        raise TypeError(msg)
    if is_dataclass(obj):
        serialized_dict = {}
        for field in fields(obj):
            field_value = getattr(obj, field.name)
            field_type = cls.__annotations__.get(field.name)
            if field_type is None or not isinstance(field_type, (type, GenericAlias)):
                msg = f"Invalid field type for {field.name} in {cls!r}"
                raise TypeError(msg)
            serialized_value = to_serializable_type(field_value, field_type)
            serialized_dict[field.name] = serialized_value
        return serialized_dict
    if not isinstance(obj, cls):
        msg = f"Object {obj!r} is not of type {cls!r}"
        raise TypeError(msg)
    return obj


def _to_serializable_list(obj: object, cls: GenericAlias) -> object:
    """
    Converts a list to a serializable format if it matches the specified generic list type.

    :param obj: List object to serialize.
    :param cls: GenericAlias representing the list type.
    :return: A serializable representation of list.
    :raise: TypeError if `obj` is not a list.
    """
    (key_type,) = get_args(cls)
    if not isinstance(obj, list):
        msg = f"Object {obj!r} is not of type {cls!r}"
        raise TypeError(msg)
    serialized_list = []
    for item in obj:
        serialized_item = to_serializable_type(item, key_type)
        serialized_list.append(serialized_item)
    return serialized_list


def _to_serializable_dict(obj: object, cls: GenericAlias) -> object:
    """
    Converts a dictionary to a serializable format if it matches the specified generic dict type.

    :param obj: Dictionary object to serialize.
    :param cls: GenericAlias representing the dict type.
    :return: A serializable representation of dictionary.
    :raise: TypeError if `obj` is not a dictionary.
    """
    key_type, value_type = get_args(cls)
    if not isinstance(obj, dict):
        msg = f"Object {obj!r} is not of type {cls!r}"
        raise TypeError(msg)
    serialized_dict = {}
    for key, value in obj.items():
        serialized_key = to_serializable_type(key, key_type)
        serialized_value = to_serializable_type(value, value_type)
        serialized_dict[serialized_key] = serialized_value
    return serialized_dict
