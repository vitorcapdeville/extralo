# Validate data with pandera

It`s highly recommended to add validation in the transformer call. This is useful to make sure the data is in the expected format, and to provide early fail if there is any unexpected change in the sources.

In the previous examples, we did not define schemas for the validation, so the validation was never executed. We can create some pandera models to validate the data:

```python title="models.py"
--8<-- "./docs_src/user/models.py"
```

And use pydantic + pandera to validate the data in the transformer:

```python title="elt.py" hl_lines="2 3 4 12 13"
--8<-- "./docs_src/user/basic_etl_with_validation.py"
```

If we run the ETL pipeline now, we will get an error because the `policy_start_date` column is not in the expected format.

```console
$ python etl.py
Traceback (most recent call last):
...
pydantic_core._pydantic_core.ValidationError: 1 validation error for my_transformer
data
  Value error, expected series 'policy_start_date' to have type datetime64[ns], got object [type=value_error, input_value=    client policy_start_d...   Eve        2024-05-05, input_type=DataFrame]
    For further information visit https://errors.pydantic.dev/2.11/v/value_error
```
Lets fix it and try again:

```python title="models.py"
--8<-- "./docs_src/user/models_fixed.py"
```

```console
$ python etl.py
```

Now the ETL pipeline should run without errors.
