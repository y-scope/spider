"""Parse TDL type string."""

from lark import Lark, LarkError, Token, Transformer, v_args

from spider.type.tdl_type import (
    BoolType,
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

primitive_type_map = {
    "bool": BoolType,
    "double": DoubleType,
    "float": FloatType,
    "int8": Int8Type,
    "int16": Int16Type,
    "int32": Int32Type,
    "int64": Int64Type,
}


class TypeTransformer(Transformer[Token, TdlType]):
    """Transform Lark parse tree into TDL type."""

    @v_args(inline=True)
    def type(self, value: TdlType) -> TdlType:
        """Unwraps the type node to return the TdlType."""
        return value

    @v_args(inline=True)
    def map_type(self, key: TdlType, value: TdlType) -> TdlType:
        """Transforms map node into Map type."""
        return MapType(key, value)

    @v_args(inline=True)
    def list_type(self, key: TdlType) -> TdlType:
        """Transforms list node into List type."""
        return ListType(key)

    def base_type(self, children: list[Token]) -> TdlType:
        """Transforms primitive node into primitive type."""
        name = str(children[0])
        if name in primitive_type_map:
            return primitive_type_map[name]()  # type: ignore[abstract]
        return ClassType(name)


parser = Lark(grammar, start="type", parser="lalr")


def parse_tdl_type(string: str) -> TdlType:
    """
    Parses TDL type string into TDL type.
    :param string: TDL type string.
    :return: Parsed TDL type.
    :raise: TypeError if string is not a valid TDL type.
    """
    try:
        tree = parser.parse(string)
        return TypeTransformer(visit_tokens=False).transform(tree)
    except LarkError as ecx:
        msg = f"Cannot parse TDL type '{string}'"
        raise TypeError(msg) from ecx
