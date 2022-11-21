import json
import os
from typing import Dict

from relationalize import Relationalize, Schema
from relationalize.utils import create_local_buffer

# This example utilizes an in-memory buffer to store the relationalized data.
# OPTIMIZATION: Schemas are generated as objects are relationalized.

FINAL_OUTPUT_DIR = "output/final"
INPUT_DIR = "example_data"

INPUT_FILENAME = "mock_lms_data.json"
OBJECT_NAME = "users"


def create_iterator(filename):
    with open(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)


# 0. Setup schemas and define on_object_write function.
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
schemas: Dict[str, Schema] = {}


def on_object_write(schema: str, object: dict):
    if schema not in schemas:
        schemas[schema] = Schema()
    schemas[schema].read_object(object)


# 1. Relationalize raw data
with Relationalize(OBJECT_NAME, create_local_buffer(), on_object_write) as r:
    r.relationalize(create_iterator(os.path.join(INPUT_DIR, INPUT_FILENAME)))

    # 2. Convert transform/flattened data to prep for database and generate SQL DDL

    for schema in schemas:
        with open(os.path.join(FINAL_OUTPUT_DIR, f"{schema}.json"), "w") as out_file:
            r.outputs[schema].seek(0)
            for line in r.outputs[schema].readlines():
                out_file.write(
                    f"{json.dumps(schemas[schema].convert_object(json.loads(line)))}\n"
                )

        with open(os.path.join(FINAL_OUTPUT_DIR, f"DDL_{schema}.sql"), "w") as ddl_file:
            ddl_file.write(schemas[schema].generate_ddl(table=schema, schema="public"))
