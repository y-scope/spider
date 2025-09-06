"""Serialization and deserialization using msgpack."""

from dataclasses import fields, is_dataclass
from types import GenericAlias
from typing import cast, get_args, get_origin


def msgpack_encoder(obj: object) -> list[object] | object:
    """
    Encodes an object into a list of its field values.
    This function recursively encodes nested dataclasses, lists, and dictionaries.
    :param obj: Dataclass instance to serialize.
    :return: List of field values.
    """
    if is_dataclass(obj):
        return {field.name: msgpack_encoder(getattr(obj, field.name)) for field in fiel
    if isinstance(obj, list):
        return [msgpack_encoder(item) for item in obj]
    if isinstance(obj, dict):
        return {msgpack_encoder(key): msgpack_encoder(val) for key, val in obj.items()}
    return obj


def _decode_class(cls: type, data: object) -> object:
    """
    Deserializes the input data as a `cls` instance.
    This function only works for non-container classes (not lists or dicts).
    :param cls: Class to deserialize into.
    :param data: Serialized data.
    :return: A new `cls` instance containing the deserialized data.
    :raise: TypeError if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create instance of {cls} with {data!r}."
    if is_dataclass(cls):
        if not isinstance(data, dict):
            raise TypeError(msg)
        args = {}
        parameters = {field.name: field for field in fields(cls)}
        for name, value in data.items():
            if name not in parameters:
                raise TypeError(msg)
            arg_cls = parameters[name].type
            if not isinstance(expected_field_type, (type, GenericAlias)):
                raise TypeError(msg)
            args[name] = msgpack_decoder(arg_cls, value)
        return cls(**args)

    return cls(data)


def msgpack_decoder(cls: type | GenericAlias, data: object) -> object:
    """
    Decodes data into an instance of `cls`.
    This function recursively decodes nested dataclasses, lists, and dictionaries.
    :param cls: Class to deserialize into.
    :param data: Data to decode.
    :return: Instance of `cls`.
    :raise: TypeError if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create instance of {cls} with {data!r}."

    origin = get_origin(cls)
    if origin is None:
        # If `cls` does not have an origin, it is a concrete type.
        return _decode_class(cast("type", cls), data)

    if origin is list:
        (key_type,) = get_args(cls)
        if not isinstance(data, list):
            raise TypeError(msg)
        return [msgpack_decoder(key_type, item) for item in data]

    if origin is dict:
        key_type, value_type = get_args(cls)
        if not isinstance(data, dict):
            raise TypeError(msg)
        return {
            msgpack_decoder(key_type, key): msgpack_decoder(value_type, value)
            for key, value in data.items()
        }

    raise TypeError(msg)
