import os

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from extralo.destinations.delta_lake import DeltaLakeDestination
from pandas.testing import assert_frame_equal


def test_delta_lake_destination_load(tmpdir):
    # Create a sample DataFrame
    initial_data = pd.DataFrame({"col1": [1, 1, 2, 2, 3, 3], "col2": ["a", "b", "c", "d", "e", "f"]})

    file_path = os.path.join(tmpdir, "test.csv")
    write_deltalake(file_path, initial_data, partition_by="col1")

    delta_lake_destination = DeltaLakeDestination(
        table_uri=file_path, mode="overwrite", partition_by="col1", partition_filters=[("col1", "=", "1")]
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
