from collections.abc import Iterable
import json
from types import TracebackType
from typing import Any, Callable, TextIO
from uuid import uuid4

from .utils import no_op, create_local_file

_DELIMITER = "_"
_ID_PREFIX = "R"
_ID = f"{_DELIMITER}rid{_DELIMITER}"
_VAL = f"{_DELIMITER}val{_DELIMITER}"
_INDEX = f"{_DELIMITER}index{_DELIMITER}"


DEFAULT_LOCAL_FILE_CALLABLE = create_local_file()

class Relationalize:
    """
    A class/utility for relationalizing JSON content.
    ```
    with Relationalize('abc') as r:
        r.relationalize([{"a": 1}])
    """

    def __init__(
        self,
        name: str,
        create_output: Callable[[str], TextIO] = DEFAULT_LOCAL_FILE_CALLABLE,
        on_object_write: Callable[[str, dict[str, Any]], None] = no_op,
    ):
        self.name = name
        self.create_output = create_output
        self.on_object_write = on_object_write
        self.outputs: dict[str, TextIO] = {}

    def __enter__(self):
        return self

    def __exit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None
    ) -> None:
        self.close_io()

    def relationalize(self, object_list: Iterable[dict[str, object]]):
        """
        Main entrypoint into this class.

        Pass in an Iterable and it will relationalize it, outputing to wherever was designated when instantiating the class.
        """
        for item in object_list:
            self._write_to_output(self.name, self._relationalize(item))

    def _write_row(self, key: str, row: dict[str, Any]):
        """
        Writes a row to the given output.
        """
        _ = self.outputs[key].write(json.dumps(row))
        _ = self.outputs[key].write("\n")
        self.on_object_write(key, row)

    def _write_to_output(
        self, key: str, content: dict[str, Any] | list[dict[str, Any]], is_sub: bool = False
    ):
        """
        Writes content, either a single object, or a list of objects to the output.

        Will create a new TextIO if needed.
        """
        identifier = f"{self.name}{_DELIMITER}{key}" if is_sub else key
        if identifier not in self.outputs:
            self.outputs[identifier] = self.create_output(identifier)
        if isinstance(content, list):
            for row in content:
                self._write_row(identifier, row)
            return
        self._write_row(identifier, content)

    def _list_helper(self, id: str, index: int, row: dict[str, object] | Any, path: str):
        """
        Helper for relationalizing lists.

        Handles the difference between an array of literals and an array of structs.
        """
        if isinstance(row, dict):
            row[_ID] = id
            row[_INDEX] = index
            return self._relationalize(row, path=path)

        return self._relationalize({_VAL: row, _ID: id, _INDEX: index}, path=path)

    def _relationalize(self, d: list[Any] | dict[str, Any] | str, path: str = ""):
        """
        Recursive back bone of the relationalize structure.

        Traverses any arbitrary JSON structure flattening and relationalizing.
        """
        path_prefix = f"{path}{_DELIMITER}"
        if path == "":
            path_prefix = ""
        if isinstance(d, list):
            id = Relationalize._generate_rid()
            for index, row in enumerate(d):
                self._write_to_output(
                    path, self._list_helper(id, index, row, path=path), is_sub=True
                )

            return {path: id}

        if isinstance(d, dict):
            temp_d: dict[str, object] = {}
            for key in d:
                temp_d.update(self._relationalize(d[key], path=f"{path_prefix}{key}"))
            return temp_d

        return {path: d}

    def close_io(self) -> None:
        for file_object in self.outputs.values():
            file_object.close()

    @staticmethod
    def _generate_rid() -> str:
        """
        Generates a relationalize ID. EX:`R_2d0418f3b5de415086f1297cf0a9d9a5`
        """
        return f"{_ID_PREFIX}{_DELIMITER}{uuid4().hex}"
