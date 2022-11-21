from typing import Dict, List

_COLUMN_SEPERATOR = "\n    , "


class SQLDialect:
    """
    Parent class for different sql dialects.

    Child classes must implement the `generate_ddl_column` method
    , and provide `type_column_mapping` and `base_ddl`.
    """

    type_column_mapping: Dict[str, str]
    base_ddl: str

    @staticmethod
    def generate_ddl_column(column_name: str, column_type: str):
        raise NotImplementedError()

    def generate_ddl(self, schema: str, table_name: str, columns: List[str]):
        """
        Generates a complete "Create Table" statement given the
        schema, table_name, and column definitions.
        """
        columns_str = _COLUMN_SEPERATOR.join(columns)
        return self.base_ddl.format(
            schema=schema, table_name=table_name, columns=columns_str
        )


class PostgresDialect(SQLDialect):
    """
    Inherits from `SQLDialect` and implements the postgres syntax.
    """

    type_column_mapping: Dict[str, str] = {
        "int": "BIGINT",
        "float": "FLOAT",
        "str": "VARCHAR(65535)",
        "bool": "BOOLEAN",
        "none": "BOOLEAN",
    }

    base_ddl: str = """
CREATE TABLE "{schema}"."{table_name}" (
    {columns}
);
    """.strip()

    @staticmethod
    def generate_ddl_column(column_name: str, column_type: str):
        cleaned_column_name = column_name.replace('"', '""')
        return f'"{cleaned_column_name}" {column_type}'
