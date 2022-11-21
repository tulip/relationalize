import json
import os
from typing import Dict

from relationalize import Relationalize, Schema
from relationalize.utils import create_local_file

# This example utilizes the local file system as a temporary storage location.

TEMP_OUTPUT_DIR = "output/temp"
FINAL_OUTPUT_DIR = "output/final"
INPUT_DIR = "example_data"

INPUT_FILENAME = "mock_lms_data.json"
OBJECT_NAME = "users"


def create_iterator(filename):
    with open(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)


def get_objects_from_dir(directory: str):
    for filename in os.listdir(directory):
        yield filename


# 0. Set up file system
os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

# 1. Relationalize raw data
with Relationalize(OBJECT_NAME, create_local_file(output_dir=TEMP_OUTPUT_DIR)) as r:
    r.relationalize(create_iterator(os.path.join(INPUT_DIR, INPUT_FILENAME)))


# 2. Generate schemas for each transformed/flattened
schemas: Dict[str, Schema] = {}
for filename in get_objects_from_dir(TEMP_OUTPUT_DIR):
    object_name, _ = os.path.splitext(filename)
    schemas[object_name] = Schema()
    for obj in create_iterator(os.path.join(TEMP_OUTPUT_DIR, filename)):
        schemas[object_name].read_object(obj)

# 3. Convert transform/flattened data to prep for database.
#    Generate SQL DDL.
for filename in get_objects_from_dir(TEMP_OUTPUT_DIR):
    object_name, _ = os.path.splitext(filename)

    with open(os.path.join(FINAL_OUTPUT_DIR, filename), "w") as out_file:
        for obj in create_iterator(os.path.join(TEMP_OUTPUT_DIR, filename)):
            converted_obj = schemas[object_name].convert_object(obj)
            serialized_row = json.dumps(converted_obj)
            out_file.write(f"{serialized_row}\n")

    with open(
        os.path.join(FINAL_OUTPUT_DIR, f"DDL_{object_name}.sql"), "w"
    ) as ddl_file:
        ddl_file.write(
            schemas[object_name].generate_ddl(table=object_name, schema="public")
        )
