import pandas as pd
import sqlalchemy as sa

from extralo.sources import SQLSource


def test_sql_source_extract():
    # Create a test database and table
    engine = sa.create_engine("sqlite:///:memory:")
    data = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Alice", "Bob"]})
    data.to_sql("test_table", engine, index=False)

    # Create an SQLSource instance
    query = "SELECT * FROM test_table"
    sql_source = SQLSource(engine, query)

    # Extract data using SQLSource
    extracted_data = sql_source.extract()

    # Assert that the extracted data matches the expected DataFrame
    assert extracted_data.equals(data)
