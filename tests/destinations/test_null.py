import pandas as pd
from extralo.destinations.null import NullDestination
from pandas.testing import assert_frame_equal


def test_null_destination_load():
    destination = NullDestination()
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    loaded_data = destination.load(data)

    assert_frame_equal(loaded_data, data)
