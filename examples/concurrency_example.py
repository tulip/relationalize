import json
import os
import time
from typing import Dict

from relationalize import Relationalize, Schema

# This example shows how concurrency in the "relationalize" and "convert_object"
# steps can be added.
# This doesn't actually add any concurrency but shows what it could look like,
# given a method of running concurrent workflows. EX: airflow.

# The general idea is:
# [r] [r] [r] [r]...
#        |
#        V
#     [merge]
#        |
#        V
# [c] [c] [c] [c]...
#        |
#        V
#      [ddl]
# where [r] is `relationalize_task` and [c] is `convert_task`

TEMP_OUTPUT_DIR = "output/temp"
FINAL_OUTPUT_DIR = "output/final"
INPUT_DIR = "example_data/sharded_mock_lms"
OBJECT_NAME = "users"


def create_iterator(filename):
    with open(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)


def get_objects_from_dir(directory: str):
    for filename in os.listdir(directory):
        yield filename


# 0. Set up file system
start_time = time.time()
os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

# 1. Define relationalize/Schema read task
def relationalize_task(file: str):
    schemas: Dict[str, Schema] = {}

    def on_object_write(schema: str, object: dict):
        if schema not in schemas:
            schemas[schema] = Schema()
        schemas[schema].read_object(object)

    def create_local_sharded_file(identifier: str):
        os.makedirs(os.path.join(TEMP_OUTPUT_DIR, identifier, "files"), exist_ok=True)
        return open(os.path.join(TEMP_OUTPUT_DIR, identifier, "files", file), "w")

    with Relationalize(OBJECT_NAME, create_local_sharded_file, on_object_write) as r:
        r.relationalize(create_iterator(os.path.join(INPUT_DIR, file)))

    for schema in schemas:
        os.makedirs(os.path.join(TEMP_OUTPUT_DIR, schema, "schemas"), exist_ok=True)
        with open(
            os.path.join(TEMP_OUTPUT_DIR, schema, "schemas", file), "w"
        ) as schema_file:
            schema_file.write(schemas[schema].serialize())

    print(f"Done relationalizing {file} | Generated {len(schemas)} schemas.")


relationalize_tasks = [file for file in os.listdir(INPUT_DIR)]

# PRETEND THAT THIS IS HAPPENING CONCURRENTLY
for file in relationalize_tasks:
    relationalize_task(file)

# 2. Merge Schemas
schemas: Dict[str, Schema] = {}

for f in os.scandir(TEMP_OUTPUT_DIR):
    if not f.is_dir():
        continue
    schema = f.name
    sharded_schemas = []
    for file in os.listdir(os.path.join(TEMP_OUTPUT_DIR, schema, "schemas")):
        sharded_schemas.append(
            json.loads(
                open(os.path.join(TEMP_OUTPUT_DIR, schema, "schemas", file), "r").read()
            )
        )
    schemas[schema] = Schema.merge(*sharded_schemas)

print(f"Done merging schemas. Found {len(schemas)} schemas accross all shards.")

# 3. Convert Objects
def convert_task(file: str, schema_name: str, schema: Schema):
    os.makedirs(os.path.join(FINAL_OUTPUT_DIR, "json", schema_name), exist_ok=True)
    with open(
        os.path.join(FINAL_OUTPUT_DIR, "json", schema_name, file), "w"
    ) as out_file:
        for object in create_iterator(
            os.path.join(TEMP_OUTPUT_DIR, schema_name, "files", file)
        ):
            out_file.write(f"{json.dumps(schema.convert_object(object))}\n")
    print(f"Converted {file} for schema {schema_name}.")


convert_tasks = []

for schema in schemas:
    for file in os.listdir(os.path.join(TEMP_OUTPUT_DIR, schema, "files")):
        convert_tasks.append((file, schema, schemas[schema]))

# PRETEND THAT THIS IS HAPPENING CONCURRENTLY
for file, schema_name, schema in convert_tasks:
    convert_task(file, schema_name, schema)

# 4. Write DDL Statements
os.makedirs(os.path.join(FINAL_OUTPUT_DIR, "sql"), exist_ok=True)
for schema in schemas:
    with open(
        os.path.join(FINAL_OUTPUT_DIR, "sql", f"DDL_{schema}.sql"), "w"
    ) as ddl_file:
        ddl_file.write(schemas[schema].generate_ddl(table=schema, schema="public"))

print(f"Wrote {len(schemas)} DDL files.")
print(f"Complete. Total Duration: {round(time.time()- start_time, 2)} seconds.")
