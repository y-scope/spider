"""Serialization and deserialization into serializable format."""

from dataclasses import fields, is_dataclass
from types import GenericAlias
from typing import Any, cast, get_args, get_origin, get_type_hints


def to_serializable(obj: object, cls: type | GenericAlias) -> object:
    """
    Converts an object into a serializable format consisting only of built-in primitive types and
    collections (lists and dictionaries).

    - Built-in container types (lists or dictionaries) are recursively transformed.
    - Dataclass instances are converted into dictionaries mapping field names to their serialized
      values.
    - All other objects are returned as-is.

    :param obj: Object to serialize. Must be of types supported by Spider TDL.
    :param cls: The expected type of `obj`. Must be a concrete type or GenericAlias supported by
        Spider TDL.
    :return: A serializable representation of `obj`.
    :raise: TypeError if `obj`'s type does not match `cls`.
    :raise: TypeError if `cls` is not a TDL supported type.
    """
    origin = get_origin(cls)
    if origin is None:
        expected_type = cast("type", cls)
        if not isinstance(obj, expected_type):
            msg = f"Object {obj!r} is not of type {expected_type!r}."
            raise TypeError(msg)
        if is_dataclass(expected_type):
            return _to_serializable_dataclass(obj, expected_type)
        return obj

    if not isinstance(obj, origin):
        msg = f"Object {obj!r} is not of type {cls!r}."
        raise TypeError(msg)

    if origin is list:
        (element_type,) = get_args(cast("GenericAlias", cls))
        return _to_serializable_list(obj, element_type)

    if origin is dict:
        (key_type, value_type) = get_args(cast("GenericAlias", cls))
        return _to_serializable_dict(obj, key_type, value_type)

    msg = f"Unsupported type: {cls!r}."
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
        expected_field_type = get_type_hints(cls).get(name)
        if not isinstance(expected_field_type, (type, GenericAlias)):
            raise TypeError(msg)
        args[name] = from_serializable(expected_field_type, value)
    return cls(**args)


def _to_serializable_dataclass(obj: object, cls: type) -> object:
    """
    Converts a dataclass to a serializable format.

    :param obj: Dataclass object to serialize.
    :param cls: The expected type of `obj`.
    :return: A serializable representation of `obj`.
    :raise: TypeError if any field in `cls` is of an unsupported type.
    """
    if not is_dataclass(obj):
        # Control flow should not reach here. However, this check is required to silence ruff.
        msg = f"Object {obj!r} is not a dataclass instance."
        raise TypeError(msg)
    serialized_dict = {}
    for field in fields(obj):
        field_value = getattr(obj, field.name)
        field_type = get_type_hints(cls).get(field.name, None)
        if field_type is None or not isinstance(field_type, (type, GenericAlias)):
            msg = f"Invalid field type for {field.name} in {cls!r}."
            raise TypeError(msg)
        serialized_value = to_serializable(field_value, field_type)
        serialized_dict[field.name] = serialized_value
    return serialized_dict


def _to_serializable_list(obj: list[Any], element_type: type | GenericAlias) -> object:
    """
    Converts a list to a serializable format.

    :param obj: List object to serialize.
    :param element_type: Type of elements in the list.
    :return: A serializable representation of the list.
    """
    serialized_list = []
    for item in obj:
        serialized_item = to_serializable(item, element_type)
        serialized_list.append(serialized_item)
    return serialized_list


def _to_serializable_dict(
    obj: dict[Any, Any], key_type: type | GenericAlias, value_type: type | GenericAlias
) -> object:
    """
    Converts a dictionary to a serializable format.

    :param obj: Dictionary object to serialize.
    :param key_type: Type of keys in the dictionary.
    :param value_type: Type of values in the dictionary.
    :return: A serializable representation of the dictionary.
    """
    serialized_dict = {}
    for key, value in obj.items():
        serialized_key = to_serializable(key, key_type)
        serialized_value = to_serializable(value, value_type)
        serialized_dict[serialized_key] = serialized_value
    return serialized_dict
