"""Parse TDL type string."""

from lark import Lark, LarkError, Token, Transformer, v_args

from spider_py.type.tdl_type import (
    BoolType,
    BytesType,
    ClassType,
    DoubleType,
    FloatType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    ListType,
    MapType,
    TdlType,
)

grammar = r"""
type: map_type | list_type | base_type

map_type: "Map" "<" type "," type ">"
list_type: "List" "<" type ">"
base_type: ID

ID: /[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*/

%import common.WS
%ignore WS
"""

PrimitiveTypeDict = {
    "bool": BoolType(),
    "bytes": BytesType(),
    "double": DoubleType(),
    "float": FloatType(),
    "int8": Int8Type(),
    "int16": Int16Type(),
    "int32": Int32Type(),
    "int64": Int64Type(),
}


class TypeTransformer(Transformer[Token, TdlType]):
    """Transforms Lark parse tree into TDL type."""

    @v_args(inline=True)
    def type(self, value_type: TdlType) -> TdlType:
        """
        Unwraps the type node to return the TdlType.
        :param value_type: The type node to unwrap.
        :return: The unwrapped TdlType.
        """
        return value_type

    @v_args(inline=True)
    def map_type(self, key_type: TdlType, value_type: TdlType) -> TdlType:
        """
        Transforms map node into Map type.
        :param key_type: The key type of the map.
        :param value_type: The value type of the map.
        :return: The MapType with the given `key_type` and `value_type`.
        """
        return MapType(key_type, value_type)

    @v_args(inline=True)
    def list_type(self, element_type: TdlType) -> TdlType:
        """
        Transforms list node into List type.
        :param element_type: The element type of the list.
        :return: The ListType with the given `element_type`.
        """
        return ListType(element_type)

    @v_args(inline=True)
    def base_type(self, token_id: str) -> TdlType:
        """
        Transforms primitive node into primitive type.
        :param token_id: The token id of the primitive type.
        :return: The corresponding TdlType of the `token_id`.
        """
        if token_id in PrimitiveTypeDict:
            return PrimitiveTypeDict[token_id]
        return ClassType(token_id)


_parser = Lark(grammar, start="type", parser="lalr")
_transformer = TypeTransformer(visit_tokens=False)


def parse_tdl_type(type_string: str) -> TdlType:
    """
    Parses TDL type string into TDL type.
    :param type_string: TDL type string.
    :return: Parsed TDL type.
    :raises TypeError: If string is not a valid TDL type.
    """
    try:
        tree = _parser.parse(type_string)
        return _transformer.transform(tree)
    except LarkError as e:
        msg = f"Cannot parse TDL type `{type_string}`"
        raise TypeError(msg) from e
