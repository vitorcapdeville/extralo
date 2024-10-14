# Add logging to pipeline

The extralo packages uses a logger named "extralo" to display useful information about the execution. You can configure the logger to display the logs in the console:

```python title="etl.py" hl_lines="1 7"
--8<-- "./docs_src/user/basic_etl_with_logging.py"
```

If we execute the ETL pipeline again, we will see some logs printed to the console:

```console
$ python etl.py
2024-10-13 22:36:38.554 | WARNING  | extralo.etl:__init__:158 - Transformer output type hints are not a TypedDict, validation will be done only at runtime.
2024-10-13 22:36:38.554 | INFO     | extralo.etl:execute:177 - Starting ETL process for basic_etl_with_logging.
2024-10-13 22:36:38.554 | INFO     | extralo.etl:_extract:77 - Starting extraction for CSVSource(file=data.csv)
2024-10-13 22:36:38.556 | INFO     | extralo.etl:_extract:79 - Extracted 5 records from CSVSource(file=data.csv)
2024-10-13 22:36:38.557 | INFO     | extralo.etl:_validate:85 - Skipping validation since no schema was provided.
2024-10-13 22:36:38.559 | INFO     | extralo.etl:transform:244 - Tranformed data with MyTransformer
2024-10-13 22:36:38.559 | INFO     | extralo.etl:_validate:85 - Skipping validation since no schema was provided.
2024-10-13 22:36:38.560 | INFO     | extralo.etl:_load:92 - Starting load of 5 records to SQLDestination(table=data_group, schema=None, if_exists=replace)
2024-10-13 22:36:38.579 | INFO     | extralo.etl:_load:94 - Loaded 5 records to SQLDestination(table=data_group, schema=None, if_exists=replace)
2024-10-13 22:36:38.580 | SUCCESS  | extralo.etl:execute:194 - ETL process for basic_etl_with_logging executed successfully.
```
