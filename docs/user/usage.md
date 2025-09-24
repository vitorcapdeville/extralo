# Basic usage

ExTraLo allows you to explicitly define the ETL pipeline. This is the simplest way to use ExTraLo. In this example, we will extract data from a csv file, transform it and load it to a SQLite database.

First, we define the transformation that will be applied to the data. The way we define this transformation is by creating a class that exposes a `transform` method. This method can receive named arguments with the same name as the name define in the `sources` definition.

In this case, we will calculate the number of days since the policy start date:

```python title="transformer.py"
--8<-- "./docs_src/user/transformer.py"
```

Notice how we defined the argument to `my_transform` function with the name `data`. This name must be the same name used in the sources definition in the next step.
Also, notice how we returned a dict of DataFrame. This is required since we could return multiple data from this step.

Lets define a SQLite database engine to use as destination:

```python hl_lines="1 6" title="etl.py"
--8<-- "./docs_src/user/basic_etl.py"
```

Now we can define the ETL pipeline. We can import the transformer we created earlier to transform the data:

```python hl_lines="2 4 8-18 20" title="etl.py"
--8<-- "./docs_src/user/basic_etl.py"
```

And finally run it:

```console
$ python etl.py
Transformer output type hints are not a TypedDict, validation will be done only at runtime.
```

And our data is extracted from the csv file, transformed and loaded to the SQLite database. We will talk about that output message in the future.

We can query the SQLite database to see the data loaded:

```python title="query.py"
--8<-- "./docs_src/user/query.py"
```

```console
$ python query.py
    client policy_start_date  days_since_start
0    Alice        2024-01-01               216
1      Bob        2024-02-02               184
2  Charlie        2024-03-03               154
3    David        2024-04-04               122
4      Eve        2024-05-05                91
```

This is the simpleste example, but ExTraLo can do more. In the next sections, you will see how to add logging and validation to the process, and also how to extract from multiple source and load to multiple destinations.
