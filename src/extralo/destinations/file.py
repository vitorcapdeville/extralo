import os.path
from abc import ABC
from typing import Any

from extralo.destination import Destination
from extralo.typing import DataFrame


class FileDestination(Destination, ABC):
    """Represents a destination that writes data to a file.

    Args:
        file (str): The path to the file.
        **kwargs: Additional keyword arguments to be passed to the load function.
    """

    def __init__(self, file: str, **kwargs: Any) -> None:
        self._file = file
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file={self._file})"


class CSVDestination(FileDestination):
    """A destination class for saving data to a CSV file."""

    def load(self, data: DataFrame) -> None:
        """Save the given DataFrame to a CSV file.

        If the file already exists, it will be overwritten.

        Args:
            data (DataFrame): The DataFrame to be saved.
        """
        data.to_csv(self._file, **self._kwargs)


class XLSXDestination(FileDestination):
    """A destination class for saving data to a XLSX file."""

    def load(self, data: DataFrame) -> None:
        """Save the given DataFrame to a XLSX file.

        If the file already exists, it will be overwritten.

        Args:
            data (DataFrame): The DataFrame to be saved.
        """
        data.to_excel(self._file, **self._kwargs)


class CSVAppendDestination(FileDestination):
    """A destination class for appending data to a CSV file."""

    def load(self, data: DataFrame) -> None:
        """Append the given DataFrame to a CSV file.

        If the file already exists, it will be appended, and will be assumed that the headers
        are in the same order.

        Args:
            data (DataFrame): The DataFrame to be saved.
        """
        if os.path.isfile(self._file):
            data.to_csv(self._file, mode="a", header=False, **self._kwargs)
            return
        data.to_csv(self._file, mode="w", header=True, **self._kwargs)
