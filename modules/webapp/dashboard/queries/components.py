from typing import Union

from dataclasses import dataclass
from dataclasses import field

from enum import Enum

@dataclass
class QuerySelect():
    __attribute : str
    __table_source: str = ""

    def __str__(self):
        prefix = f"{self.__table_source}." if self.__table_source != "" else ""
        return f"{prefix}{self.__attribute}"

    def __eq__(self, other):
        return\
            (self.__attribute == other.attribute)\
            and (self.__table_source == other.table_source)

    @property
    def attribute(self):
        return self.__attribute

    @property
    def table_source(self):
        return self.__table_source

@dataclass
class QueryTable():
    __schema: str
    __name: str
    __alias: str = ""

    def __str__(self):
        return f"{self.__schema}.{self.__name} {self.__alias}".strip()

    def __eq__(self, other):
        return\
            (self.__schema == other.schema)\
            and (self.__name == other.name)\
            and (self.__alias == other.alias)

    @property
    def schema(self):
        return self.__schema

    @property
    def name(self):
        return self.__name

    @property
    def alias(self):
        return self.__alias

@dataclass
class QueryFrom():
    __table_source: QueryTable
    __alias: str = ""

    def __str__(self):
        return f"{self.__table_source} {self.__alias}".strip()

    def __eq__(self, other):
        return\
            (self.__table_source == other.__table_source)\
            and (self.__alias == other.__alias)

    @property
    def table_source(self):
        return self.__table_source

    @property
    def alias(self):
        return self.__alias


class JoinType(Enum):
    Left = 0
    Right = 1
    Inner = 2


@dataclass
class QueryJoin():
    __ltable: QueryTable
    __rtable: QueryTable
    __join_type: JoinType
    __on_attribute: str

    def __str__(self):
        if self.__join_type is JoinType.Left:
            rtable_repr = str(self.__rtable)
            return f"LEFT JOIN {rtable_repr} ON {self.__ltable.alias}.{self.__on_attribute} = {self.__rtable.alias}.{self.__on_attribute}"
        elif self.__join_type is JoinType.Right:
            return f"RIGHT JOIN {rtable_repr} ON {self.__ltable.alias}.{self.__on_attribute} = {self.__rtable.alias}.{self.__on_attribute}"
        elif self.__join_type is JoinType.Inner:
            return f"INNER JOIN {rtable_repr} ON {self.__ltable.alias}.{self.__on_attribute} = {self.__rtable.alias}.{self.__on_attribute}"
        else:
            raise ValueError("Invalid Join Type!")

    def __eq__(self, other):
        return\
            (self.__ltable.name == other.ltable.name)\
            and (self.__ltable.schema == other.ltable.schema)\
            and (self.__rtable.name == other.rtable.name)\
            and (self.__rtable.schema == other.rtable.schema)\
            and (self.__join_type == other.join_type)\
            and (self.__on_attribute == other.on_attribute)

    @property
    def ltable(self):
        return self.__ltable

    @property
    def rtable(self):
        return self.__rtable

    @property
    def join_type(self):
        return self.__join_type

    @property
    def on_attribute(self):
        return self.__on_attribute

@dataclass
class QueryWhere():
    __condition : str

    def __str__(self):
        return self.__condition

    def __eq__(self, other):
        return (self.__condition == other.condition)