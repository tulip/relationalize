import json
from typing import Any, Final, Generic, TypeVar, cast

from relationalize.types import BaseSupportedColumnType, ChoiceColumnType, ColumnType, UnsupportedColumnType, is_choice_column_type

from .sql_dialects import PostgresDialect, SQLDialect

DialectColumnType = TypeVar('DialectColumnType')

ALLOWED_COLUMN_CHARS: Final[set[str]] = {" ", "-", "_"}
DEFAULT_SQL_DIALECT = PostgresDialect()

class Schema(Generic[DialectColumnType]):
    """
    A choice-supporting schema for a flattened JSON object.
    """

    _CHOICE_SEQUENCE: str = "c-"
    _CHOICE_DELIMITER: str = "-"

    def __init__(
        self,
        schema: dict[str, ColumnType] | None = None,
        sql_dialect: SQLDialect[DialectColumnType] = DEFAULT_SQL_DIALECT,
    ):
        if schema is None:
            schema = dict()
        self.schema = schema
        self.sql_dialect = sql_dialect

    def convert_object(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Convert a given object according to the schema.
        Splits choice-columns into N seperate columns and renames keys accordingly.

        Chooses between schema-iteration and object-iteration depending on which one will be more efficient.
        """
        if len(self.schema) > len(record):
            return self._convert_object_object_iteration(record)
        return self._convert_object_schema_iteration(record)

    def _convert_object_schema_iteration(
        self, record: dict[str, object]
    ) -> dict[str, object]:
        output_object: dict[str, object] = {}
        for key, value_type in self.schema.items():
            if key not in record:
                continue
            object_value = record[key]
            if object_value is None:
                output_object[key] = object_value
                continue
            if is_choice_column_type(value_type):
                # determine which type this object is and enter into correct sub-column
                object_value_type = self._parse_type(object_value)
                if object_value_type not in value_type:
                    raise Exception(
                        (
                            "Unknown type found within object. But not within the schema.\n"
                            f"schema types: {value_type}\n"
                            f"object type: {object_value_type}"
                        )
                    )
                output_object[f"{key}_{object_value_type}"] = object_value
                continue
            # Non-Choice column. Do Nothing.
            output_object[key] = object_value
        return output_object

    def _convert_object_object_iteration(
        self, record: dict[str, object]
    ) -> dict[str, object]:
        output_object: dict[str, object] = {}
        for key, object_value in record.items():
            if object_value is None:
                output_object[key] = object_value
                continue
            if key not in self.schema:
                continue
            value_type = self.schema[key]
            if is_choice_column_type(value_type):
                # determine which type this object is and enter into correct sub-column
                object_value_type = self._parse_type(object_value)
                if object_value_type not in value_type:
                    raise Exception(
                        (
                            "Unknown type found within object. But not within the schema.\n"
                            f"schema types: {value_type}\n"
                            f"object type: {object_value_type}"
                        )
                    )
                output_object[f"{key}_{object_value_type}"] = object_value
                continue
            # noop. no choice found.
            output_object[key] = object_value
        return output_object

    def generate_output_columns(self) -> list[str]:
        """
        Generates the columns that will be in the output of `convert_object`
        """
        columns: list[str] = []
        for key, value_type in self.schema.items():
            if Schema._CHOICE_SEQUENCE not in value_type:
                # Column is not a choice column
                columns.append(key)
                continue
            # Generate a column per choice-type
            for choice_type in value_type[2:].split(Schema._CHOICE_DELIMITER):
                if choice_type == "none":
                    continue
                columns.append(f"{key}_{choice_type}")
        columns.sort()
        return columns

    def generate_ddl(self, table: str, schema: str = "public") -> str:
        """
        Generates a CREATE TABLE statement for this schema.
        Breaking out choice columns into seperate columns.
        """
        columns: list[str] = []
        for key, value_type in self.schema.items():
            if Schema._CHOICE_SEQUENCE not in value_type:
                # Column is not a choice column
                columns.append(
                    self.sql_dialect.generate_ddl_column(
                        key, self.sql_dialect.type_column_mapping[value_type]
                    )
                )
                continue
            # Generate a column per choice-type
            for choice_type in cast(list[BaseSupportedColumnType], value_type[2:].split(Schema._CHOICE_DELIMITER)):
                if choice_type == "none":
                    continue
                columns.append(
                    self.sql_dialect.generate_ddl_column(
                        f"{key}_{choice_type}",
                        self.sql_dialect.type_column_mapping[choice_type],
                    )
                )
        columns.sort()
        return self.sql_dialect.generate_ddl(schema, table, columns)

    def drop_null_columns(self) -> int:
        """
        Drops none-typed columns from the schema.

        Returns the # of columns that were dropped.
        """
        columns_to_drop: list[str] = []
        for key, value in self.schema.items():
            if value == "none":
                columns_to_drop.append(key)

        for column in columns_to_drop:
            del self.schema[column]
        return len(columns_to_drop)

    def drop_special_char_columns(self, allowed_chars: set[str] = ALLOWED_COLUMN_CHARS) -> int:
        """
        Drops columns which have a non alnumeric in their name from the schema.
        Optional input a set of allowed_chars to define any additional characters which are allowed.
        By default this includes spaces, dashes, and underscores.

        Returns the # of columns that were dropped.
        """
        columns_to_drop: list[str] = []
        for key in self.schema.keys():
            if any(not (c.isalnum() or c in allowed_chars) for c in key):
                columns_to_drop.append(key)

        for column in columns_to_drop:
            del self.schema[column]
        return len(columns_to_drop)

    def drop_duplicate_columns(self) -> int:
        """
        Drops columns from the schema which have a duplicate (case sensitive) match. Keeps the first column it reads.

        Returns the # of columns that were dropped.
        """
        lowercased_keys: set[str] = set()
        columns_to_drop: list[str] = []
        for key in self.schema.keys():
            if key.casefold() not in lowercased_keys:
                lowercased_keys.add(key.casefold())
            else:
                columns_to_drop.append(key)

        for column in columns_to_drop:
            del self.schema[column]
        return len(columns_to_drop)

    def read_object(self, record: dict[str, object]):
        """
        Read an object and merge into the current schema.
        """
        for key, value in record.items():
            self._read_write_object_key(key, value)

    def serialize(self) -> str:
        """
        Serialize this schema to a string.
        """
        return json.dumps(self.schema)

    @staticmethod
    def deserialize(content: str):
        """
        Create a new Schema class instance from a serialized schema.
        """
        return Schema(schema=json.loads(content))

    def _read_write_object_key(self, key: str, value: object):
        value_type = Schema._parse_type(value)
        if key not in self.schema:
            # Key has not been encountered yet. Set type in schema to type of value.
            self.schema[key] = value_type
            return
        if self.schema[key] == value_type:
            # Entry in schema for this key has same type as this record. Do Nothing.
            return
        if self.schema[key] == "none":
            # Entry in schema for this key is `none`. Set type in schema to type of value.
            self.schema[key] = value_type
            return
        # Entry in schema exists for this key and the type for value is different.
        if value_type == "none":
            # Value type is `none` but existing entry in schema exists. Do Nothing.
            return
        if self.schema[key][:2] == Schema._CHOICE_SEQUENCE:
            # Entry in schema is a choice column.
            if value_type in self.schema[key]:
                # Type for Value is already in the schema choice pattern. Do Nothing.
                return

            # Add this type into the choice pattern.
            self.schema[key] = ChoiceColumnType(f"{self.schema[key]}{Schema._CHOICE_DELIMITER}{value_type}")

            choices = self.schema[key].split(Schema._CHOICE_DELIMITER)[1:]
            # Remove `none` type from choices
            if "none" in choices:
                choices.remove("none")
            # Check if choices is only of lenth 1 and remove choice pattern.
            if len(choices) == 1:
                self.schema[key] = cast(BaseSupportedColumnType, choices[0])
                return
            # Reorder the types so things are predictable.
            self.schema[
                key
            ] = ChoiceColumnType(f"{Schema._CHOICE_SEQUENCE}{Schema._CHOICE_DELIMITER.join(sorted(choices))}")
            return

        # Create new 2-type choice pattern.
        self.schema[
            key
        ] = ChoiceColumnType(f"{Schema._CHOICE_SEQUENCE}{Schema._CHOICE_DELIMITER.join(sorted([self.schema[key], value_type]))}")

    @staticmethod
    def merge(*args: dict[str, ColumnType]):
        """
        Create a new Schema object from multiple serialized schemas merging them together.
        """
        merged_schema: dict[str, ColumnType] = {}
        for schema in args:
            for key, value_type in schema.items():
                if key not in merged_schema:
                    merged_schema[key] = value_type
                    continue
                if value_type == merged_schema[key]:
                    continue

                # key is in the new schema already and has different type
                choices: set[str] = set()
                if Schema._CHOICE_SEQUENCE in merged_schema[key]:
                    for t in merged_schema[key][2:].split(Schema._CHOICE_DELIMITER):
                        if t == "none":
                            continue
                        choices.add(t)
                else:
                    choices.add(merged_schema[key])
                if Schema._CHOICE_SEQUENCE in value_type:
                    for t in value_type[2:].split(Schema._CHOICE_DELIMITER):
                        if t == "none":
                            continue
                        choices.add(t)
                else:
                    choices.add(value_type)

                if "none" in choices:
                    choices.remove("none")
                if len(choices) == 0:
                    merged_schema[key] = "none"
                    continue
                if len(choices) == 1:
                    merged_schema[key] = cast(BaseSupportedColumnType, choices.pop())
                    continue

                merged_schema[
                    key
                ] = ChoiceColumnType(f"{Schema._CHOICE_SEQUENCE}{Schema._CHOICE_DELIMITER.join(sorted(choices))}")
        return Schema(schema=merged_schema)

    @staticmethod
    def _parse_type(value: object) -> ColumnType:
        """
        Get the type of a given value
        """
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "str"
        if value is None:
            return "none"
        return UnsupportedColumnType(f"unsupported:{type(value)}")
