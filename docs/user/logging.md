# Add logging to pipeline

The extralo packages uses a logger named "elt" to display useful information about the execution. You can configure the logger to display the logs in the console:

```python title="etl.py" hl_lines="1 7"
--8<-- "./docs_src/user/basic_etl_with_logging.py"
```

If we execute the ETL pipeline again, we will see some logs printed to the console:

```console
$ python etl.py
WARNING:etl:Transformer output type hints are not a TypedDict, validation will be done only at runtime.
INFO:etl:Starting extraction for CSVSource(file=data.csv)
INFO:etl:Extracted 5 records from CSVSource(file=data.csv)
INFO:etl:Tranformed data with MyTransformer
INFO:etl:Starting load of 5 records to SQLDestination(table=data_group, schema=None, if_exists=replace)
INFO:etl:Loaded 5 records to SQLDestination(table=data_group, schema=None, if_exists=replace)
```

Preharps the best way to configure the logger is to create a `logging.config` file and load it in the beginning of the script.

```json title="logging.json"
--8<-- "./docs_src/user/logging.json"
```

```python title="etl.py" hl_lines="1 2 8-11"
--8<-- "./docs_src/user/basic_etl_with_logging_config.py"
```
If we execute the pipeline again, we will see the formatted logs printed to the console:

```console
$ python etl.py
2024-08-04 09:05:40 | etl | WARNING  - Transformer output type hints are not a TypedDict, validation will be done only at runtime.
2024-08-04 09:05:40 | etl | INFO     - Starting extraction for CSVSource(file=data.csv)
2024-08-04 09:05:40 | etl | INFO     - Extracted 5 records from CSVSource(file=data.csv)
2024-08-04 09:05:40 | etl | INFO     - Tranformed data with MyTransformer
2024-08-04 09:05:40 | etl | INFO     - Starting load of 5 records to SQLDestination(table=data_group, schema=None, if_exists=replace)
2024-08-04 09:05:40 | etl | INFO     - Loaded 5 records to SQLDestination(table=data_group, schema=None, if_exists=replace)
```

Also, notice that in the configuration we set two handlers to the `etl` logger: file and stdout. The stdout handler is the responsible for printing the logs to the console. The file handler also send our log messages to a file named `etl.log`.

Further configuration for the logging messages can be done using the features in the [logging](https://docs.python.org/3/library/logging.html) module.
