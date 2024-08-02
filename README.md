# ETL using python

Python package for extracting data from a source, transforming it and loading it to a destination, with validation in between.

The provided ETL pipeline provides useful functionality on top of the usual operations:

- **Extract**: Extract data from multiples sources, in parallel (using threads).
- **Validate**: Validate the extracted data, to make sure it matches what will be required by the transform step, using pandera schemas. This provide early fail if there is any unexpected change in the sources.
- **Transform**: Define the logic for transformation of the data, making it reusable, and allowing multiple data frames as input and multiple data frames as output.
- **Validate again**: Validate the transformed data, to make sure it matches your expectation, and what the destination will require.
- **Load**: Load multiple data, each to one or more destination, and load diferent data to diferent destinations in parallel (using threads).

## Installation

The package is available at PyPI, so you can install it using pip:

```bash
pip install extralo
```

## Usage

Lets create some fake data to use in the examples:

```python
import pandas as pd

data = pd.DataFrame(
    {
        "client": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "policy_start_date": ["2024-01-01", "2024-02-02", "2024-03-03", "2024-04-04", "2024-05-05"],
    }
)

data.to_csv("data.csv", index=False)
```

Lets define some logic to transform the data:

```python
from extralo.transformer import Transformer


class MyTransformer(Transformer):
    def transform(self, data):
        data["policy_start_date"] = pd.to_datetime(data["policy_start_date"])
        data["days_since_start"] = (pd.Timestamp.now() - data["policy_start_date"]).dt.days
        return {"data": data}
```

Notice how we defined the argument to transform with the name "data". This name must be the same name used in the sources definition in the next step.
Also, notice how we returned a dict of DataFrame. This is required since we could return multiple data from this step.

Lets create a SQLite database to use as destination:

```python
from sqlalchemy import create_engine

# Create a SQLite database
engine = create_engine("sqlite:///data.sqlite")
```

Now we can define the ETL pipeline:

```python
from extralo import ETL, CSVSource, SQLDestination
import pandera as pa

etl = ETL(
    sources={
        "data": CSVSource("data.csv"),
    },
    before_schemas={"data": pa.DataFrameModel},
    transformer=MyTransformer(),
    after_schemas={
        "data": pa.DataFrameModel,
    },
    destinations={
        "data": [
            SQLDestination(engine, "data_group", None, if_exists="replace"),
        ],
    },
)
```

And finally run it:

```python
etl.execute()
```

### Log the execution

The extralo packages uses a logger named "elt" to display useful information about the execution. You can configure the logger to display the logs in the console:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

If we execute the ETL pipeline again, we will see some logs printed to the console:

```python
etl.execute()
```

The log message can be configured using the functionality provided by the [logging](https://docs.python.org/3/library/logging.html) module.

### Validate data with pandera

The ETL pipeline can validate the data extracted and transformed using pandera schemas. This is useful to make sure the data is in the expected format, and to provide early fail if there is any unexpected change in the sources.

In the previous example, we used the "base" DataFrameModel to create a validator that will never fail. We can create a more strict schema to validate the data:

```python
before_schema = pa.DataFrameSchema(
    {
        "client": pa.Column(pa.String),
        "policy_start_date": pa.Column(pa.DateTime),
    }
)

after_schema = pa.DataFrameSchema(
    {
        "client": pa.Column(pa.String),
        "policy_start_date": pa.Column(pa.DateTime),
        "days_since_start": pa.Column(pa.Int),
    }
)
```

And inform the ETL pipeline to use these schemas:

```python
etl = ETL(
    sources={
        "data": CSVSource("data.csv"),
    },
    before_schemas={"data": before_schema},
    transformer=MyTransformer(),
    after_schemas={
        "data": after_schema,
    },
    destinations={
        "data": [
            SQLDestination(engine, "data_group", None, if_exists="replace"),
        ],
    },
)

etl.execute()
```

Notice that we got SchemaErrors (since the validation is always performed with `lazy=True`): `policy_start_date` is not a `DateTime`.
Lets fix it and try again:

```python
before_schema = pa.DataFrameSchema(
    {
        "client": pa.Column(pa.String),
        "policy_start_date": pa.Column(pa.String),
    }
)

etl = ETL(
    sources={
        "data": CSVSource("data.csv"),
    },
    before_schemas={"data": before_schema},
    transformer=MyTransformer(),
    after_schemas={
        "data": after_schema,
    },
    destinations={
        "data": [
            SQLDestination(engine, "data_group", None, if_exists="replace"),
        ],
    },
)

etl.execute()
```
