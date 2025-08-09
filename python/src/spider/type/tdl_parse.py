"""Parse TDL type string."""

from copy import copy

from lark import Token, Transformer, Tree, v_args

from spider.type.tdl_type import (
    BoolType,
    ClassType,
    DoubleType,
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

map_type: "Map<" type_type "," type_type ">"
list_type: "List<" type_type ">"
base_type: ID

ID: /[A-Za-z_][A-Za-z0-9_]*/

%import common.WS
%ignore WS
"""

primitive_type_map = {
    "bool": BoolType(),
    "double": DoubleType(),
    "int8": Int8Type(),
    "int16": Int16Type(),
    "int32": Int32Type(),
    "int64": Int64Type(),
}


class TypeTransformer(Transformer[Tree[Token], TdlType]):
    """Transform Lark tree into TDL type."""

    @v_args(inline=True)
    def map_type(self, key: TdlType, value: TdlType) -> TdlType:
        """Transforms map node into Map type."""
        return MapType(key, value)

    @v_args(inline=True)
    def list_type(self, key: TdlType) -> TdlType:
        """Transforms list node into Map type."""
        return ListType(key)

    def base_type(self, children: list[Token]) -> TdlType:
        """Transforms primitive node into primitive type."""
        name = str(children[0])
        if name in primitive_type_map:
            return copy(primitive_type_map[name])
        return ClassType(name)
