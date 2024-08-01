from typing import Literal, NewType, TypeGuard

"""
unsupported:[type]
"""
UnsupportedColumnType = NewType('UnsupportedColumnType', str)

def is_unsupported_column_type(column: str) -> TypeGuard[UnsupportedColumnType]:
    return column.startswith('unsupported:')

"""
c-{hypen-delimited choice type list}
"""
ChoiceColumnType = NewType('ChoiceColumnType', str)

def is_choice_column_type(column: str) -> TypeGuard[ChoiceColumnType]:
    return column.startswith('c-')

BaseSupportedColumnType = Literal[
    'bool',
    'datetime',
    'float',
    'int',
    'none',
    'str',
]
SupportedColumnType = BaseSupportedColumnType | ChoiceColumnType

ColumnType = SupportedColumnType | UnsupportedColumnType
