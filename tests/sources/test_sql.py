import pandas as pd
import pytest
import sqlalchemy as sa

from extralo.sources import SQLSource


@pytest.fixture
def engine_with_data():
    engine = sa.create_engine("sqlite:///:memory:")
    data = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Alice", "Bob"]})
    data.to_sql("test_table", engine, index=False)
    return engine, data


def test_sql_source_extract(engine_with_data):
    engine, data = engine_with_data

    query = "SELECT * FROM test_table"
    sql_source = SQLSource(engine, query)

    extracted_data = sql_source.extract()

    assert extracted_data.equals(data)


def test_sql_source_extract_with_scalar_params(engine_with_data):
    engine, data = engine_with_data

    query = "SELECT * FROM test_table WHERE name = :name"
    sql_source = SQLSource(engine, query, params={"name": "Alice"})

    extracted_data = sql_source.extract()

    expected = data[data["name"] == "Alice"].reset_index(drop=True)
    assert extracted_data.equals(expected)


def test_sql_source_extract_with_list_param(engine_with_data):
    engine, data = engine_with_data

    query = "SELECT * FROM test_table WHERE id IN :ids"
    sql_source = SQLSource(engine, query, params={"ids": [1, 3]})

    extracted_data = sql_source.extract()

    expected = data[data["id"].isin([1, 3])].reset_index(drop=True)
    assert extracted_data.equals(expected)


def test_sql_source_extract_with_list_and_scalar_params(engine_with_data):
    engine, data = engine_with_data

    query = "SELECT * FROM test_table WHERE id IN :ids AND name = :name"
    sql_source = SQLSource(engine, query, params={"ids": [1, 2, 3], "name": "Bob"})

    extracted_data = sql_source.extract()

    expected = data[(data["id"].isin([1, 2, 3])) & (data["name"] == "Bob")].reset_index(drop=True)
    assert extracted_data.equals(expected)


def test_sql_source_extract_with_empty_list_param(engine_with_data):
    engine, _data = engine_with_data

    query = "SELECT * FROM test_table WHERE id IN :ids"
    sql_source = SQLSource(engine, query, params={"ids": []})

    extracted_data = sql_source.extract()

    assert extracted_data.empty


def test_sql_source_extract_with_multiple_list_params(engine_with_data):
    engine, data = engine_with_data

    query = "SELECT * FROM test_table WHERE id IN :ids AND name IN :names"
    sql_source = SQLSource(engine, query, params={"ids": [1, 2], "names": ["John", "Alice"]})

    extracted_data = sql_source.extract()

    expected = data[(data["id"].isin([1, 2])) & (data["name"].isin(["John", "Alice"]))].reset_index(drop=True)
    assert extracted_data.equals(expected)
