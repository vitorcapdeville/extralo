# Add logging to pipeline

The extralo packages uses a logger named "extralo" to display useful information about the execution. You can configure the logger to display the logs in the console:

```python title="etl.py" hl_lines="1 7"
--8<-- "./docs_src/user/basic_etl_with_logging.py"
```

If we execute the ETL pipeline again, we will see some logs printed to the console:

```console
$ python etl.py
2025-04-26 17:16:09.573 | INFO     | extralo.etl:execute:147 - Starting ETL process for basic_etl_with_logging.
2025-04-26 17:16:09.573 | INFO     | extralo.etl:_extract:71 - Starting extraction for CSVSource(file=data.csv)
2025-04-26 17:16:09.575 | INFO     | extralo.etl:_extract:73 - Extracted 5 records from CSVSource(file=data.csv)
2025-04-26 17:16:09.577 | INFO     | extralo.etl:transform:202 - Tranformed data with <function my_transformer at 0x70eb71b36f20>
2025-04-26 17:16:09.578 | INFO     | extralo.etl:_load:78 - Starting load of 5 records to SQLDestination(table=data_group, schema=None, if_exists=replace)
2025-04-26 17:16:09.598 | INFO     | extralo.etl:_load:80 - Loaded 5 records to SQLDestination(table=data_group, schema=None, if_exists=replace)
2025-04-26 17:16:09.598 | SUCCESS  | extralo.etl:execute:166 - ETL process for basic_etl_with_logging executed successfully.
```
