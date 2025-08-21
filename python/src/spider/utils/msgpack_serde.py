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
        return [msgpack_encoder(getattr(obj, f.name)) for f in fields(obj)]
    if isinstance(obj, list):
        return [msgpack_encoder(item) for item in obj]
    if isinstance(obj, dict):
        return {msgpack_encoder(k): msgpack_encoder(v) for k, v in obj.items()}
    return obj


def _decode_class(cls: type, data: object) -> object:
    """
    Decodes data into an instance of a `cls`.
    This function only works for non-container classes (not lists or dicts).
    :param cls: Class to deserialize into.
    :param data: Data to decode.
    :return: Instance of `cls`.
    :raise: TypeError if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create instance of {cls} with {data!r}"
    if is_dataclass(cls):
        if not isinstance(data, list):
            raise TypeError(msg)
        parameters = fields(cls)
        if len(data) != len(parameters):
            raise TypeError(msg)
        args = []
        for param, value in zip(parameters, data, strict=False):
            if not isinstance(param.type, type):
                raise TypeError(msg)
            args.append(msgpack_decoder(param.type, value))
        return cls(*args)

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
    msg = f"Cannot create instance of {cls} with {data!r}"

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
            msgpack_decoder(key_type, k): msgpack_decoder(value_type, v) for k, v in data.items()
        }

    raise TypeError(msg)
