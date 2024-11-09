import os

import pandas as pd
from deltalake import write_deltalake
from pandas.testing import assert_frame_equal

from extralo.sources.delta_lake import DeltaLakeSource, SparkDeltaLakeSource


def test_delta_lake_source_extract(tmpdir):
    # Create a sample DataFrame
    initial_data = pd.DataFrame({"col1": [1, 1, 2, 2, 3, 3], "col2": ["a", "b", "c", "d", "e", "f"]})

    file_path = os.path.join(tmpdir, "test.csv")
    write_deltalake(file_path, initial_data, partition_by="col1")

    delta_lake_source = DeltaLakeSource(table_uri=file_path, partitions=[("col1", "=", "2")])

    obtained_data = delta_lake_source.extract()

    expected_data = pd.DataFrame({"col1": [2, 2], "col2": ["c", "d"]})

    assert_frame_equal(obtained_data, expected_data)


def test_spark_delta_lake_source_extract(spark):
    # Create a sample DataFrame
    initial_data = pd.DataFrame({"col1": [1, 1, 2, 2, 3, 3], "col2": ["a", "b", "c", "d", "e", "f"]})

    spark.createDataFrame(initial_data).write.format("delta").mode("overwrite").saveAsTable("test_extract_table")

    delta_lake_source = SparkDeltaLakeSource(spark=spark, query="select * from test_extract_table where col1 = 2")

    obtained_data = delta_lake_source.extract()

    expected_data = pd.DataFrame({"col1": [2, 2], "col2": ["c", "d"]})

    assert_frame_equal(
        obtained_data.sort_values("col2").reset_index(drop=True),
        expected_data.sort_values("col2").reset_index(drop=True),
    )
