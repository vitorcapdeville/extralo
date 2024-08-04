from abc import ABC
from typing import Any

from extralo.source import Source
from extralo.typing import DataFrame


class FileSource(Source, ABC):
    """Represents a file source.

    Args:
        file (str): The path to the file.
        **kwargs: Additional keyword arguments to be passed to the read function.
    """

    def __init__(self, file: str, **kwargs: Any) -> None:
        self._file = file
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file={self._file})"


class CSVSource(FileSource):
    """A class representing a CSV data source.

    This class inherits from the FileSource class and provides a method to extract data from a CSV file.
    """

    def __init__(self, file: str, **kwargs: Any) -> None:
        try:
            import pandas  # noqa: F401
        except ImportError as err:
            raise ImportError(
                "Pandas is required to use CSVSource. Please install it with `pip install pandas`."
            ) from err
        super().__init__(file, **kwargs)

    def extract(self) -> DataFrame:
        """Extracts data from a CSV file.

        Returns:
            DataFrame: The extracted data as a pandas DataFrame.
        """
        import pandas as pd

        data: pd.DataFrame = pd.read_csv(self._file, **self._kwargs)
        return data


class XLSXSource(FileSource):
    """A class representing an XLSX data source.

    This class inherits from the FileSource class and provides a method to extract data from a XLSX file.
    """

    def __init__(self, file: str, **kwargs: Any) -> None:
        try:
            import pandas  # noqa: F401
        except ImportError as err:
            raise ImportError(
                "Pandas is required to use XLSXSource. Please install it with `pip install pandas`."
            ) from err
        super().__init__(file, **kwargs)

    def extract(self) -> DataFrame:
        """Extracts data from an Excel file.

        Returns:
            DataFrame: The extracted data.
        """
        import pandas as pd

        data: pd.DataFrame = pd.read_excel(self._file, **self._kwargs)
        return data
