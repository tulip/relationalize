import os
from io import StringIO


def create_local_file(output_dir: str = ""):
    """
    A `create_output` compatible Callable for utilizing the local File System with relationalize.
    """

    def open_local_file(identifier: str):
        return open(
            f"{os.path.join(output_dir, identifier)}.json",
            "w",
            buffering=1,
        )

    return open_local_file


def create_local_buffer():
    """
    A `create_output` compatible Callable that creates in memory buffers.
    """

    def open_local_buffer(identifier: str):
        return StringIO()

    return open_local_buffer


def no_op(schema: str, object: dict[str, object]) -> None:
    """
    Does nothing.
    """
    pass
