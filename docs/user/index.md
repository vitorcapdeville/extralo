# User guide

The user guide shows how to use the extralo package to create ETL pipelines.

To run the examples in this section, you will need some data. You can create a CSV file with fake data using the following code:

```python title="fake_data.py"
--8<-- "./docs_src/user/fake_data.py"
```

Run the code in the console:

```console
$ python fake_data.py
```

A CSV file will be created in your current directory with the name `data.csv`.

```csv title="data.csv"
client,policy_start_date
Alice,2024-01-01
Bob,2024-02-02
Charlie,2024-03-03
David,2024-04-04
Eve,2024-05-05
```
