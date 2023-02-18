import json
from typing import Any, Dict, Optional

from .sql_dialects import PostgresDialect, SQLDialect


class Schema:
    """
    A choice-supporting schema for a flattened JSON object.
    """

    _CHOICE_SEQUENCE: str = "c-"
    _CHOICE_DELIMITER: str = "-"

    def __init__(
        self,
        schema: Optional[dict] = None,
        sql_dialect: SQLDialect = PostgresDialect(),
    ):
        if schema is None:
            schema = {}
        self.schema: Dict[str, str] = schema
        self.sql_dialect = sql_dialect

    def convert_object(self, object: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a given object according to the schema.
        Splits choice-columns into N seperate columns and renames keys accordingly.

        Chooses between schema-iteration and object-iteration depending on which one will be more efficient.
        """
        if len(self.schema) > len(object):
            return self._convert_object_object_iteration(object)
        return self._convert_object_schema_iteration(object)

    def _convert_object_schema_iteration(
        self, object: Dict[str, Any]
    ) -> Dict[str, Any]:
        output_object = {}
        for key, value_type in self.schema.items():
            if key not in object:
                continue
            object_value = object[key]
            if object_value is None:
                output_object[key] = object_value
                continue
            if Schema._CHOICE_SEQUENCE in value_type:
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
        self, object: Dict[str, Any]
    ) -> Dict[str, Any]:
        output_object = {}
        for key, object_value in object.items():
            if object_value is None:
                output_object[key] = object_value
                continue
            if key not in self.schema:
                continue
            value_type = self.schema[key]
            if Schema._CHOICE_SEQUENCE in value_type:
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

    def generate_output_columns(self):
        """
        Generates the columns that will be in the output of `convert_object`
        """
        columns = []
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
        columns = []
        for key, value_type in self.schema.items():
            if Schema._CHOICE_SEQUENCE not in value_type:
                # Column is not a choice column
                columns.append(
                    self.sql_dialect.generate_ddl_column(
                        key.casefold(), self.sql_dialect.type_column_mapping[value_type]
                    )
                )
                continue
            # Generate a column per choice-type
            for choice_type in value_type[2:].split(Schema._CHOICE_DELIMITER):
                if choice_type == "none":
                    continue
                columns.append(
                    self.sql_dialect.generate_ddl_column(
                        f"{key.casefold()}_{choice_type}",
                        self.sql_dialect.type_column_mapping[choice_type],
                    )
                )
        # Because SQL is case sensitive and JSON is not
        # Remove duplicate columns after enforcing lowercase
        if len(columns) > 0:
            deduped_columns = list(set(columns)).sort()
        else:
            deduped_columns = columns
        return self.sql_dialect.generate_ddl(schema, table, deduped_columns)

    def read_object(self, object: Dict):
        """
        Read an object and merge into the current schema.
        """
        for key, value in object.items():
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

    def _read_write_object_key(self, key, value):
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
            self.schema[key] += f"{Schema._CHOICE_DELIMITER}{value_type}"

            choices = self.schema[key].split(Schema._CHOICE_DELIMITER)[1:]
            # Remove `none` type from choices
            if "none" in choices:
                choices.remove("none")
            # Check if choices is only of lenth 1 and remove choice pattern.
            if len(choices) == 1:
                self.schema[key] = choices[0]
                return
            # Reorder the types so things are predictable.
            self.schema[
                key
            ] = f"{Schema._CHOICE_SEQUENCE}{Schema._CHOICE_DELIMITER.join(sorted(choices))}"
            return

        # Create new 2-type choice pattern.
        self.schema[
            key
        ] = f"{Schema._CHOICE_SEQUENCE}{Schema._CHOICE_DELIMITER.join(sorted([self.schema[key], value_type]))}"

    @staticmethod
    def merge(*args: Dict[str, str]):
        """
        Create a new Schema object from multiple serialized schemas merging them together.
        """
        merged_schema: Dict[str, str] = {}
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
                    merged_schema[key] = choices.pop()
                    continue

                merged_schema[
                    key
                ] = f"{Schema._CHOICE_SEQUENCE}{Schema._CHOICE_DELIMITER.join(sorted(choices))}"
        return Schema(schema=merged_schema)

    @staticmethod
    def _parse_type(value):
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
        return f"unsupported:{type(value)}"
