# Relatinize
transforming arbitrary collections of JSON objects, into a relational-friendly format. Draws inspiration from https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-transforms-Relationalize.html
Relationize is a Python library for transforming arbitrary collections of JSON objects, into a relational-friendly format. It draws inspiration from https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-transforms-Relationalize.html
## Why Relationize
Relationize is a replacement for AWS Glue Relationalize, and it provides  more portability, saves on cost, and bypasses limitations of Glue Relationalize (Column limit)
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
To run them cd into the examples folder. 
## Using Relationalize with Airflow
Relationize also comes with an already built airflow helper. 

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