import os

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from pandas.testing import assert_frame_equal

from extralo.destinations.delta_lake import DeltaLakeDestination, SparkDeltaLakeDestination


def test_delta_lake_destination_load(tmpdir):
    # Create a sample DataFrame
    initial_data = pd.DataFrame({"col1": [1, 1, 2, 2, 3, 3], "col2": ["a", "b", "c", "d", "e", "f"]})

    file_path = os.path.join(tmpdir, "test")
    write_deltalake(file_path, initial_data, partition_by=None)

    delta_lake_destination = DeltaLakeDestination(
        table_uri=file_path, mode="overwrite", partition_by=None, predicate="col1 = 1"
    )

    data = pd.DataFrame({"col1": [1, 1, 1], "col2": ["aa", "bbb", "cccc"]})

    # Call the load method
    delta_lake_destination.load(data)
    delta_lake_destination.load(data)
    delta_lake_destination.load(data)

    obtained_data = DeltaTable(file_path).to_pandas()
    expected_data = pd.DataFrame({"col1": [1, 1, 1, 2, 2, 3, 3], "col2": ["aa", "bbb", "cccc", "c", "d", "e", "f"]})

    assert_frame_equal(
        obtained_data.sort_values("col1").reset_index(drop=True),
        expected_data.sort_values("col1").reset_index(drop=True),
    )


def test_spark_delta_lake_destination_load(spark):
    # Create a sample DataFrame
    initial_data = pd.DataFrame({"col1": [1, 1, 2, 2, 3, 3], "col2": ["a", "b", "c", "d", "e", "f"]})

    spark.createDataFrame(initial_data).write.format("delta").mode("overwrite").saveAsTable("test_table")

    delta_lake_destination = SparkDeltaLakeDestination(
        spark=spark, table="test_table", mode="overwrite", partition_by=None, replace_where="col1 = 1"
    )

    data = pd.DataFrame({"col1": [1, 1, 1], "col2": ["aa", "bbb", "cccc"]})

    # Call the load method
    delta_lake_destination.load(data)
    delta_lake_destination.load(data)
    delta_lake_destination.load(data)

    obtained_data = spark.sql("SELECT * FROM test_table").toPandas()
    expected_data = pd.DataFrame({"col1": [1, 1, 1, 2, 2, 3, 3], "col2": ["aa", "bbb", "cccc", "c", "d", "e", "f"]})

    assert_frame_equal(
        obtained_data.sort_values("col2").reset_index(drop=True),
        expected_data.sort_values("col2").reset_index(drop=True),
    )
