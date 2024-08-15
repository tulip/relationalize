from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Generic, Literal, NewType, TypeVar

from relationalize.types import SupportedColumnType

_COLUMN_SEPARATOR = "\n    , "

DDLColumn = NewType('DDLColumn', str)
DialectColumnType = TypeVar('DialectColumnType')

class SQLDialect(ABC, Generic[DialectColumnType]):
    """
    Parent class for different sql dialects.

    Child classes must implement the `generate_ddl_column` method
    , and provide `type_column_mapping` and `base_ddl`.
    """

    type_column_mapping: Mapping[SupportedColumnType, DialectColumnType]
    base_ddl: str

    @staticmethod
    @abstractmethod
    def generate_ddl_column(column_name: str, column_type: DialectColumnType) -> DDLColumn:
        raise NotImplementedError()

    def generate_ddl(self, schema: str, table_name: str, columns: list[str]):
        """
        Generates a complete "Create Table" statement given the
        schema, table_name, and column definitions.
        """
        columns_str = _COLUMN_SEPARATOR.join(columns)
        return self.base_ddl.format(
            schema=schema, table_name=table_name, columns=columns_str
        )


PostgresColumn = Literal[
    'BIGINT',
    'BOOLEAN',
    'FLOAT',
    'TIMESTAMP',
    'VARCHAR(65535)',
]

class PostgresDialect(SQLDialect[PostgresColumn]):
    """
    Inherits from `SQLDialect` and implements the postgres syntax.
    """

    type_column_mapping: Mapping[SupportedColumnType, PostgresColumn] = {
        "int": "BIGINT",
        "datetime": "TIMESTAMP",
        "float": "FLOAT",
        "str": "VARCHAR(65535)",
        "bool": "BOOLEAN",
        "none": "BOOLEAN",
    }

    base_ddl: str = """
CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
    {columns}
);
    """.strip()

    @staticmethod
    def generate_ddl_column(column_name: str, column_type: PostgresColumn):
        cleaned_column_name = column_name.replace('"', '""')
        return DDLColumn(f'"{cleaned_column_name}" {column_type}')
