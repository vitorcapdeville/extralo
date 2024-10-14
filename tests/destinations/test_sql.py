import pandas as pd
import pytest
import sqlalchemy as sa
from pandas.testing import assert_frame_equal

from extralo.destinations.sql import SQLAppendDestination, SQLDestination


def test_sql_destination_load():
    # Create a test database and table
    engine = sa.create_engine("sqlite:///:memory:")
    data = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Alice", "Bob"]})

    # Create an SQLDestination instance
    table_name = "test_table"
    schema = None
    if_exists = "replace"
    sql_destination = SQLDestination(engine, table_name, schema, if_exists)

    # Load data using SQLDestination
    sql_destination.load(data)

    # Get the loaded data from the database
    loaded_data = pd.read_sql("SELECT * FROM test_table", engine)

    # Assert that the loaded data matches the expected DataFrame
    assert loaded_data.equals(data)

    # Assert that the table exists in the database
    data_loaded_via_query = pd.read_sql("SELECT * FROM test_table", engine)
    assert_frame_equal(data_loaded_via_query, data)


def test_sql_append_destination_load():
    # Create a test database and table
    engine = sa.create_engine("sqlite:///:memory:")
    data = pd.DataFrame({"id": [2, 2, 2], "name": ["John", "Alice", "Bob"]})
    data.to_sql("test_table", engine, index=False)

    # Create an SQLAppendDestination instance
    table = "test_table"
    schema = None
    group_column = "id"
    group_value = 2
    sql_append_destination = SQLAppendDestination(engine, table, schema, group_column, group_value)

    # Load data using SQLAppendDestination
    sql_append_destination.load(data)
    sql_append_destination.load(data)

    # Get the loaded data from the database
    loaded_data = pd.read_sql("SELECT * FROM test_table", engine)

    # Assert that the loaded data matches the expected DataFrame
    assert_frame_equal(loaded_data, data)


def test_sql_append_destination_load_fails_if_group_columns_does_not_exist():
    # Create a test database and table
    engine = sa.create_engine("sqlite:///:memory:")
    data = pd.DataFrame({"id": [2, 2, 2], "name": ["John", "Alice", "Bob"]})
    data.to_sql("test_table", engine, index=False)

    # Create an SQLAppendDestination instance
    table = "test_table"
    schema = None
    group_column = "not_there"
    group_value = 2
    sql_append_destination = SQLAppendDestination(engine, table, schema, group_column, group_value)

    with pytest.raises(KeyError, match="not found"):
        sql_append_destination.load(data)
