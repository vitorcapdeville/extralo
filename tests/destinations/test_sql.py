import pandas as pd
import sqlalchemy as sa
from extralo.destinations.sql import SQLDestination


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
    loaded_data = sql_destination.load(data)

    # Assert that the loaded data matches the expected DataFrame
    assert loaded_data.equals(data)

    # Assert that the table exists in the database
    data_loaded_via_query = pd.read_sql("SELECT * FROM test_table", engine)
    assert data_loaded_via_query.equals(data)
