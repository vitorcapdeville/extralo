import os

import pandas as pd
from pandas.testing import assert_frame_equal

from extralo.destinations import CSVAppendDestination, CSVDestination, XLSXDestination


def test_csv_append_destination_load(tmpdir):
    # Create a DataFrame for testing
    data = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    # Load the data into the CSVAppendDestination
    file_path = os.path.join(tmpdir, "test.csv")
    CSVAppendDestination(file_path, index=False).load(data)

    # Verify that the file was created and contains the expected data
    file_path = os.path.join(tmpdir, "test.csv")
    assert os.path.isfile(file_path)

    loaded_data = pd.read_csv(file_path)
    assert_frame_equal(loaded_data, data)

    CSVAppendDestination(file_path, index=False).load(data)

    loaded_data = pd.read_csv(file_path)
    assert_frame_equal(loaded_data, pd.concat((data, data)).reset_index(drop=True))


def test_csv_destination_load(tmpdir):
    # Create a DataFrame for testing
    data = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    # Load the data into the CSVAppendDestination
    file_path = os.path.join(tmpdir, "test.csv")
    CSVDestination(file_path, index=False).load(data)

    # Verify that the file was created and contains the expected data
    file_path = os.path.join(tmpdir, "test.csv")
    assert os.path.isfile(file_path)

    loaded_data = pd.read_csv(file_path)
    assert_frame_equal(loaded_data, data)


def test_xlsx_destination_load(tmpdir):
    # Create a DataFrame for testing
    data = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    # Load the data into the CSVAppendDestination
    file_path = os.path.join(tmpdir, "test.xlsx")
    XLSXDestination(file_path, index=False).load(data)

    # Verify that the file was created and contains the expected data
    file_path = os.path.join(tmpdir, "test.xlsx")
    assert os.path.isfile(file_path)

    loaded_data = pd.read_excel(file_path)
    assert_frame_equal(loaded_data, data)
