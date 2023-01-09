import csv
import json
import os
import time
from typing import Dict
from uuid import uuid4

import psycopg2
import pymongo
from relationalize import Relationalize, Schema
from relationalize.utils import create_local_file

# This example shows an entire pipeline built that moves data from a MongoDB collection into a postgres DB.
# External Dependencies:
# pymongo 4.3.3
# psycopg2 2.9.5

### CONSTANTS ###
MONGO_HOST = ""
MONGO_USERNAME = ""
MONGO_PASSWORD = ""
MONGO_DB = ""
MONGO_COLLECTION = ""

PG_HOST = ""
PG_PORT = 5432
PG_USERNAME = ""
PG_PASSWORD = ""
PG_DB = "postgres"
PG_SCHEMA = "public"


RUN_ID = uuid4().hex[:8]

LOCAL_FS_PATH = os.path.join("output", RUN_ID)
LOCAL_EXPORT_LOCATION = os.path.join(LOCAL_FS_PATH, "export")
LOCAL_TEMP_LOCATION = os.path.join(LOCAL_FS_PATH, "temp")
LOCAL_FINAL_LOCATION = os.path.join(LOCAL_FS_PATH, "final")

EXPORT_PATH = os.path.join(LOCAL_EXPORT_LOCATION, f"{MONGO_COLLECTION}.json")


### SETUP ###
start_time = time.time()
os.makedirs(LOCAL_FS_PATH, exist_ok=True)
os.makedirs(LOCAL_EXPORT_LOCATION, exist_ok=True)
os.makedirs(LOCAL_TEMP_LOCATION, exist_ok=True)
os.makedirs(LOCAL_FINAL_LOCATION, exist_ok=True)

schemas: Dict[str, Schema] = {}


def on_object_write(schema: str, object: dict):
    if schema not in schemas:
        schemas[schema] = Schema()
    schemas[schema].read_object(object)


def create_iterator(filename):
    with open(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)


### EXPORT DATA FROM MongoDB ###
MONGO_CONNECTION_STRING = f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}/?retryWrites=true&w=majority"
db = pymongo.MongoClient(MONGO_CONNECTION_STRING)[MONGO_DB]

print("-" * 20)
print(f"Exporting {MONGO_COLLECTION} from {MONGO_HOST} into {EXPORT_PATH}")
with open(EXPORT_PATH, "w") as export_file:
    for document in db[MONGO_COLLECTION].find():
        export_file.write(f"{json.dumps(document, default=str)}\n")
export_checkpoint = time.time()


### RELATIONALIZE ###
print("-" * 20)
print(f"Relationalizing {MONGO_COLLECTION} from local file: {EXPORT_PATH}")
with Relationalize(
    MONGO_COLLECTION, create_local_file(LOCAL_TEMP_LOCATION), on_object_write
) as r:
    r.relationalize(create_iterator(EXPORT_PATH))
relationalize_checkpoint = time.time()

### CONVERT OBJECTS ###
print("-" * 20)
print(f"Converting objects for {len(schemas)} relationalized schemas.")
conversion_durations: Dict[str, float] = {}
for schema_name, schema in schemas.items():
    conversion_start_time = time.time()
    print(
        (
            f"Converting objects for schema {schema_name}. "
            f"Reading from {LOCAL_TEMP_LOCATION}/{schema_name}.json "
            f"Writing to {LOCAL_FINAL_LOCATION}/{schema_name}.csv"
        )
    )
    with open(
        os.path.join(LOCAL_FINAL_LOCATION, f"{schema_name}.csv"),
        "w",
    ) as final_file:
        writer = csv.DictWriter(final_file, fieldnames=schema.generate_output_columns())
        writer.writeheader()
        for row in create_iterator(
            os.path.join(LOCAL_TEMP_LOCATION, f"{schema_name}.json")
        ):
            converted_obj = schema.convert_object(row)
            writer.writerow(converted_obj)
    conversion_durations[schema_name] = time.time() - conversion_start_time
conversion_checkpoint = time.time()


### COPY TO POSTGRES ###
print("-" * 20)
print((f"Copying data to Postgres using {PG_HOST} " f"DB {PG_DB} SCHEMA {PG_SCHEMA}"))
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USERNAME,
    password=PG_PASSWORD,
)

cursor = conn.cursor()
upload_durations: Dict[str, float] = {}
upload_row_counts: Dict[str, int] = {}
for schema_name, schema in schemas.items():
    print(f"Copying data for schema {schema_name}.")
    upload_start_time = time.time()
    drop_table_statement = f'DROP TABLE IF EXISTS "{PG_SCHEMA}"."{schema_name}";'
    create_table_statement = schema.generate_ddl(table=schema_name, schema=PG_SCHEMA)
    analyze_statement = f'ANALYZE "{PG_SCHEMA}"."{schema_name}";'
    count_statement = f'SELECT COUNT(1) FROM "{PG_SCHEMA}"."{schema_name}";'

    print("Executing drop table statement.")
    print(drop_table_statement)
    cursor.execute(drop_table_statement)
    conn.commit()

    print("Executing create table statement.")
    print(create_table_statement)
    cursor.execute(create_table_statement)
    conn.commit()

    print("Executing copy statement.")
    with open(
        os.path.join(LOCAL_FINAL_LOCATION, f"{schema_name}.csv"), "r"
    ) as final_file:
        cursor.copy_expert(
            f"COPY {PG_SCHEMA}.{schema_name} from STDIN DELIMITER ',' CSV HEADER;",
            final_file,
        )
    conn.commit()

    print("Executing analyze statement.")
    print(analyze_statement)
    cursor.execute(analyze_statement)
    conn.commit()

    print("Executing count statement.")
    print(count_statement)
    cursor.execute(count_statement)
    count_result = cursor.fetchone()
    upload_row_counts[schema_name] = count_result[0] if count_result else -1
    conn.commit()

    upload_durations[schema_name] = time.time() - upload_start_time

upload_checkpoint = time.time()
print("-" * 20)
print(
    f"Data transformation/transfer complete. Created {len(schemas)} tables in Postgres:"
)
for schema_name in schemas:
    print(f'"{PG_SCHEMA}"."{schema_name}"')
print("-" * 20)

print(f"Export duration: {round(export_checkpoint - start_time, 2)} seconds.")
print(
    f"Relationalize duration: {round(relationalize_checkpoint - export_checkpoint, 2)} seconds."
)
print(
    f"Conversion duration: {round(conversion_checkpoint - relationalize_checkpoint, 2)} seconds."
)
print(
    f"Postgres copy duration: {round(upload_checkpoint - conversion_checkpoint, 2)} seconds."
)

print(f"Total duration: {round(upload_checkpoint - start_time, 2)} seconds.")
print("-" * 20)
print("Object Details:")
for schema_name in sorted(schemas.keys(), key=lambda x: len(x)):
    print(
        (
            f"{schema_name}. Column Count: {len(schemas[schema_name].schema)} "
            f"Row Count: {upload_row_counts[schema_name]} "
            f"Conversion Duration: {round(conversion_durations[schema_name], 2)} seconds. "
            f"Upload Duration: {round(upload_durations[schema_name], 2)}"
        )
    )
