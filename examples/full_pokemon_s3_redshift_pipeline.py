import json
import time
from datetime import datetime
from typing import Any, Dict, Generator
from uuid import uuid4

import boto3
import redshift_connector
import requests
import simplejson
import smart_open
from relationalize import Relationalize, Schema

# This example shows an entire pipeline built utilizing the pokeAPI, s3, and redshift.
# External Dependencies:
# smart_open==6.2.0
# redshift-connector==2.0.909
# simplejson==3.18.0
# requests==2.28.1
#
# This example assumes that it provided with AWS credentials.
# For more information please see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
# These credentials need Read/Write Access for the given S3 location.
#
# Additionally this example assumes that a redshift cluster exists,
# and has the requisite iam_role attached with permissions to copy data from the given S3 bucket.
#
# This example utilizes s3 as an intermediate storage location in order to mock how a pipeline might be built
# where different steps don't have access to the same local file system

### CONSTANTS ###
REDSHIFT_HOST = ""
REDSHIFT_PORT = 5439
REDSHIFT_USERNAME = ""
REDSHIFT_PASSWORD = ""
REDSHIFT_DB = ""
REDSHIFT_SCHEMA = ""
REDSHIFT_IAM_ROLE = ""
REDSHIFT_REGION = "us-east-1"


## If you dont change the export path or temp_location it will create the following "folders" in S3:
## pokemon/XXX_YYYYMMDD/
## pokemon/XXX_YYYYMMDD/temp/*
S3_BUCKET = ""
S3_EXPORT_PATH = f"pokemon/{uuid4().hex[:8]}_{datetime.now().strftime('%Y%m%d')}/"
S3_TEMP_LOCATION = f"{S3_BUCKET}/{S3_EXPORT_PATH}temp/"


OBJECT_NAME = "pokemon"

### SETUP ###
start_time = time.time()
s3_client = boto3.client("s3")
s3_temp_location = (
    f"{S3_BUCKET}/{uuid4().hex[:8]}_temp/"
    if S3_TEMP_LOCATION is None
    else S3_TEMP_LOCATION
)
s3_final_location = f"{s3_temp_location}final/"

s3_export_file_path = f"{S3_EXPORT_PATH}{OBJECT_NAME}.json"

schemas: Dict[str, Schema] = {}


# reducing the min_part_size from the default 50mb to 5mb (minimum allowed) reduces the memory usage.
def wopen(
    path: str, mode: str = "w", s3_client=s3_client, min_part_size: int = 5 * 1024**2
):
    return smart_open.open(
        path,
        mode,
        transport_params={"client": s3_client, "min_part_size": min_part_size},
    )


def on_object_write(schema: str, object: dict):
    if schema not in schemas:
        schemas[schema] = Schema()
    schemas[schema].read_object(object)


def create_s3_file_iterator(filename: str) -> Generator[Dict[str, Any], None, None]:
    with wopen(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)


def create_relationalize_s3_file(identifier: str):
    return wopen(
        f"s3://{s3_temp_location}intermediate/{identifier}.json",
        "w",
    )


### EXPORT DATA FROM API ###
print("-" * 20)
print(f"Exporting pokemon from pokeAPI into S3 {S3_BUCKET}/{s3_export_file_path}")
with wopen(f"s3://{S3_BUCKET}/{s3_export_file_path}", "w") as export_file:
    list_of_pokemon = requests.get(
        "https://pokeapi.co/api/v2/pokemon?limit=100000&offset=0"
    ).json()["results"]
    for index, pokemon in enumerate(list_of_pokemon, start=1):
        pokemon_data = requests.get(pokemon["url"]).json()
        export_file.write(f"{json.dumps(pokemon_data)}\n")
        if index % 100 == 0:
            print(f"Exported {index} / {len(list_of_pokemon)} pokemon...")
export_checkpoint = time.time()


### RELATIONALIZE ###
print("-" * 20)
print(
    f"Relationalizing {OBJECT_NAME} from remote file: {S3_BUCKET}/{s3_export_file_path}"
)
with Relationalize(OBJECT_NAME, create_relationalize_s3_file, on_object_write) as r:
    r.relationalize(create_s3_file_iterator(f"s3://{S3_BUCKET}/{s3_export_file_path}"))
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
            f"Reading from {s3_temp_location}intermediate/{schema_name}.json "
            f"Writing to {s3_final_location}{schema_name}.json.gz"
        )
    )
    with wopen(
        f"s3://{s3_final_location}{schema_name}.json.gz",
        "w",
    ) as final_file:
        for row in create_s3_file_iterator(
            f"s3://{s3_temp_location}intermediate/{schema_name}.json"
        ):
            final_file.write(
                f"{simplejson.dumps(schema.convert_object(row), ignore_nan=True)}\n"
            )
    conversion_durations[schema_name] = time.time() - conversion_start_time
conversion_checkpoint = time.time()

### COPY TO REDSHIFT ###
print("-" * 20)
print(
    (
        f"Copying data from S3 to Redshift using cluster {REDSHIFT_HOST} "
        f"DB {REDSHIFT_DB} SCHEMA {REDSHIFT_SCHEMA} IAM ROLE {REDSHIFT_IAM_ROLE}"
    )
)
conn = redshift_connector.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    database=REDSHIFT_DB,
    user=REDSHIFT_USERNAME,
    password=REDSHIFT_PASSWORD,
)
cursor = conn.cursor()
upload_durations: Dict[str, float] = {}
upload_row_counts: Dict[str, int] = {}
for schema_name, schema in schemas.items():
    print(f"Copying data for schema {schema_name}.")
    upload_start_time = time.time()
    drop_table_statement = f'DROP TABLE IF EXISTS "{REDSHIFT_SCHEMA}"."{schema_name}";'
    create_table_statement = schema.generate_ddl(
        table=schema_name, schema=REDSHIFT_SCHEMA
    )
    copy_statement = f"""
COPY "{REDSHIFT_SCHEMA}"."{schema_name}"
FROM 's3://{s3_final_location}{schema_name}.json.gz'
iam_role '{REDSHIFT_IAM_ROLE}'
region '{REDSHIFT_REGION}'
FORMAT AS json 'auto ignorecase'
TRUNCATECOLUMNS
GZIP;
    """.strip()
    analyze_statement = f'ANALYZE "{REDSHIFT_SCHEMA}"."{schema_name}";'
    count_statement = f'SELECT COUNT(1) FROM "{REDSHIFT_SCHEMA}"."{schema_name}";'

    print("Executing drop table statement.")
    print(drop_table_statement)
    cursor.execute(drop_table_statement)
    conn.commit()

    print("Executing create table statement.")
    print(create_table_statement)
    cursor.execute(create_table_statement)
    conn.commit()

    print("Executing copy statement.")
    print(copy_statement)
    cursor.execute(copy_statement)
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
    f"Data transformation/transfer complete. Created {len(schemas)} tables in redshift:"
)
for schema_name in schemas:
    print(f'"{REDSHIFT_SCHEMA}"."{schema_name}"')
print("-" * 20)

print(f"Export duration: {round(export_checkpoint - start_time, 2)} seconds.")
print(
    f"Relationalize duration: {round(relationalize_checkpoint - export_checkpoint, 2)} seconds."
)
print(
    f"Conversion duration: {round(conversion_checkpoint - relationalize_checkpoint, 2)} seconds."
)
print(
    f"Redshift copy duration: {round(upload_checkpoint - conversion_checkpoint, 2)} seconds."
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
