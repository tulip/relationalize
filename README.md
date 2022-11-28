# Relatinize

Relationize is a Python library for transforming arbitrary JSON and putting it into a relational database.
## Why Relationize
Relationize is a replacement for AWS Glue which allows you to transform and place the arbitrary JSON without incurring large AWS Glue fee. 

## How Relationize works
The relationalize function recursively navigates the JSON object and splits out new objects whenever an array is encountered and provides a connection/relation between the objects. You can provide the Relationalize class a function which will determine where to write the transformed content. This could be a local file object, a remote (s3) file object, or an in memory buffer.

The relationalize class constructor takes in a function that produces a TextIO object when given a name. This function is used to create the outputs for the base object, and any relationalized objects that are created. It is important that we be able to dynamically generate these output locations because we know nothing about the structure of the data we are transforming.

As the relationalize function walks the tree of the JSON document it is first flattening any sub-structs and second converting any arrays.



## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install relationize.

```bash
pip install relationize
```

## Example
Example relationize usages are placed in the examples/ folder. 
##### Single Threaded (Local FS) Example.

```python
import json
import os
from typing import Dict

from relationalize import Relationalize, create_local_file
from schema import Schema

TEMP_OUTPUT_DIR = "output/temp"
FINAL_OUTPUT_DIR = "output/final"
OBJECT_NAME = "test"
INPUT_FILENAME = "test.json"


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
    r.relationalize(create_iterator(INPUT_FILENAME))

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
        ddl_file.write(schemas[object_name].generate_ddl(table=object_name))
```
## Using Relationalize with Airflow
Relationize also comes with an already built airflow helper. 

In order to optimize this process we can do both the relationalizing and converting steps with multiple concurrent workers. This looks like:
```
               +-----------+
               |Chunk Files|
               +-----|-----+
                     |
                     |
+--------------------|--------------------+
|              Relationalize              |
| +-----------+ +----------+ +----------+ |
| | Task 0    | | Task 1   | |  Task 2  | |
| |           | |          | |          | |
| +-----------+ +----------+ +----------+ |
+--------------------|--------------------+
                     |
             +-------|------+
             |Merge Schemas |
             +-------|------+
                     |
                     |
+--------------------|--------------------+
|              Convert Objects            |
| +-----------+ +----------+ +----------+ |
| | Task 0    | | Task 1   | |  Task 2  | |
| |           | |          | |          | |
| +-----------+ +----------+ +----------+ |
+--------------------|--------------------+
                     |
                     |
            +--------|--------+
            | Redshift Upload |
            +-----------------+
```

1. First we chunk the files that we we want to convert into N groups, where N is the # of parallel relationalize tasks.
2. In each Relationalize Task we run the relationalize function and direct the output to a temporary locaiton in s3. We utilize the `on_object_write` to generate a partial schema of all new relationalized objects so that we don't have to iterate over them again just to generate the schema. These partial schemas and relationalized files are both written to s3.
3. Read all of the partial schemas from s3 and merge them together. Push the resultant schemas to airflow xcom so that they can be accessed in later steps. Also chunk out the files in the temp s3 bucket into N groups where N is the # of parallel convert tasks.
4. In each Convert Task we deserialize the schemas from xcom, stream data from the temp s3 location convert each row, and then stream back the converted data back to the final s3 location.
5. Deserialize the schemas from xcom, generate DDL statements, generate COPY statements, and run DDL and COPY statements against the redshift DB.

Example implementation within a DAG using the `relationalize_helper`:
```python
relationalize_task_group = relationalize_helper(
        bucket='co.tulip.jira-export',
        object_name="issues",
        prefix="issues",
        concurrency=4,
        redshift_schema='jira_new,
    )
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)