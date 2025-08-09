"""Parse TDL type string."""

from copy import copy

from lark import Lark, Token, Transformer, v_args, Tree

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
    TdlType
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
    "bool": BoolType(),
    "double": DoubleType(),
    "float": FloatType(),
    "int8": Int8Type(),
    "int16": Int16Type(),
    "int32": Int32Type(),
    "int64": Int64Type(),
}


class TypeTransformer(Transformer[Token, TdlType]):
    """Transform Lark parse tree into TDL type."""

    @v_args(inline=True)
    def map_type(self, key: Tree, value: Tree) -> TdlType:
        """Transforms map node into Map type."""
        return MapType(key.children[0], value.children[0])

    @v_args(inline=True)
    def list_type(self, key: Tree) -> TdlType:
        """Transforms list node into Map type."""
        return ListType(key.children[0])

    def base_type(self, children: list[Token]) -> TdlType:
        """Transforms primitive node into primitive type."""
        name = str(children[0])
        if name in primitive_type_map:
            return copy(primitive_type_map[name])
        return ClassType(name)

parser = Lark(grammar, start="type", parser="lalr")

def parse_tdl_type(string: str) -> TdlType:
    """
    Parses TDL type string into TDL type.
    :param string: TDL type string.
    :return: Parsed TDL type.
    :raise: TypeError if string is not a valid TDL type.
    """
    tree = parser.parse(string)
    try:
        return TypeTransformer(visit_tokens=False).transform(tree).children[0]
    except IndexError:
        raise TypeError(f"'{string}' is not a valid TDL type.")
