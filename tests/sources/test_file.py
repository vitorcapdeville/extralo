import pandas as pd

from extralo.sources import CSVSource, XLSXSource


def test_csv_source_extract(tmp_path):
    # Create a temporary CSV file for testing
    csv_file = tmp_path / "test.csv"
    data = {"Name": ["John", "Alice", "Bob"], "Age": [25, 30, 35], "City": ["New York", "London", "Paris"]}
    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=False)

    # Create a CSVSource instance
    csv_source = CSVSource(csv_file)

    # Extract data using CSVSource
    extracted_data = csv_source.extract()

    # Assert that the extracted data matches the original DataFrame
    assert extracted_data.equals(df)


def test_xlsx_source_extract(tmp_path):
    # Create a temporary XLSX file for testing
    xlsx_file = tmp_path / "test.xlsx"
    data = {"Name": ["John", "Alice", "Bob"], "Age": [25, 30, 35], "City": ["New York", "London", "Paris"]}
    df = pd.DataFrame(data)
    df.to_excel(xlsx_file, index=False)

    # Create an XLSXSource instance
    xlsx_source = XLSXSource(xlsx_file)

    # Extract data using XLSXSource
    extracted_data = xlsx_source.extract()

    # Assert that the extracted data matches the original DataFrame
    assert extracted_data.equals(df)
