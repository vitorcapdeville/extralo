# type: ignore
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import pandas as pd

T = TypeVar("T")


class FileSource(ABC, Generic[T]):
    """Represents a file source.

    Args:
        file (str): The path to the file.
        **kwargs: Additional keyword arguments to be passed to the read function.
    """

    def __init__(self, file: str, **kwargs: Any) -> None:
        self._file = file
        self._kwargs = kwargs

    @abstractmethod
    def extract(self) -> T:
        """Extracts data from a file.

        Returns:
            T: The extracted data.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file={self._file})"


class CSVSource(FileSource[pd.DataFrame]):
    """A class representing a CSV data source.

    This class inherits from the FileSource class and provides a method to extract data from a CSV file.
    """

    def extract(self) -> pd.DataFrame:
        """Extracts data from a CSV file.

        Returns:
            DataFrame: The extracted data as a pandas DataFrame.
        """
        data: pd.DataFrame = pd.read_csv(self._file, **self._kwargs)
        return data


class XLSXSource(FileSource[pd.DataFrame]):
    """A class representing a XLSX data source.

    This class inherits from the FileSource class and provides a method to extract data from a XLSX file.
    """

    def extract(self) -> pd.DataFrame:
        """Extracts data from an Excel file.

        Returns:
            DataFrame: The extracted data.
        """
        data: pd.DataFrame = pd.read_excel(self._file, **self._kwargs)
        return data


class SASSource(FileSource[pd.DataFrame]):
    """A class representing a SAS data source.

    This class inherits from the FileSource class and provides a method to extract data from a SAS file.
    """

    def extract(self) -> pd.DataFrame:
        """Extracts data from a SAS file.

        Returns:
            DataFrame: The extracted data.
        """
        return pd.read_sas(self._file, **self._kwargs)


class JSONSource(FileSource[pd.DataFrame]):
    """A class representing a JSON data source.

    This class inherits from the FileSource class and provides a method to extract data from a JSON file.
    """

    def extract(self) -> pd.DataFrame:
        """Extracts data from a JSON file.

        Returns:
            DataFrame: The extracted data.
        """
        return pd.read_json(self._file, **self._kwargs)
