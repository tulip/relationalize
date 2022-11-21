import json
from typing import Any, Callable, Dict, Iterable, List, TextIO, Union
from uuid import uuid4

from .utils import _no_op, create_local_file

_DELIMITER = "_"
_ID_PREFIX = "R"
_ID = f"{_DELIMITER}rid{_DELIMITER}"
_VAL = f"{_DELIMITER}val{_DELIMITER}"
_INDEX = f"{_DELIMITER}index{_DELIMITER}"


class Relationalize:
    """
    A class/utility for relationalizing JSON content.
    ```
    with Relationalize('abc') as r:
        r.relationalize([{"a": 1}])
    """

    def __init__(
        self,
        name,
        create_output: Callable[[str], TextIO] = create_local_file(),
        on_object_write: Callable[[str, Dict[str, Any]], None] = _no_op,
    ):
        self.name = name
        self.create_output = create_output
        self.on_object_write = on_object_write
        self.outputs: Dict[str, TextIO] = {}

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close_io()

    def relationalize(self, object_list: Iterable[dict]):
        """
        Main entrypoint into this class.

        Pass in an Iterable and it will relationalize it, outputing to wherever was designated when instantiating the class.
        """
        for item in object_list:
            self._write_to_output(self.name, self._relationalize(item))

    def _write_row(self, key: str, row: Dict[str, Any]):
        """
        Writes a row to the given output.
        """
        self.outputs[key].write(json.dumps(row))
        self.outputs[key].write("\n")
        self.on_object_write(key, row)

    def _write_to_output(
        self, key: str, content: Union[Dict, List[Dict]], is_sub=False
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

    def _list_helper(self, id, index, row, path):
        """
        Helper for relationalizing lists.

        Handles the difference between an array of literals and an array of structs.
        """
        if isinstance(row, dict):
            row[_ID] = id
            row[_INDEX] = index
            return self._relationalize(row, path=path)

        return self._relationalize({_VAL: row, _ID: id, _INDEX: index}, path=path)

    def _relationalize(self, d, path=""):
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
            temp_d = {}
            for key in d:
                temp_d.update(self._relationalize(d[key], path=f"{path_prefix}{key}"))
            return temp_d

        return {path: d}

    def close_io(self):
        for file_object in self.outputs.values():
            file_object.close()

    @staticmethod
    def _generate_rid():
        """
        Generates a relationalize ID. EX:`R_2d0418f3b5de415086f1297cf0a9d9a5`
        """
        return f"{_ID_PREFIX}{_DELIMITER}{uuid4().hex}"
