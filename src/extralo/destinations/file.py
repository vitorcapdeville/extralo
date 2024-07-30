import os.path
from abc import ABC

from extralo.destination import Destination
from extralo.typing import DataFrame


class FileDestination(Destination, ABC):
    def __init__(self, file: str, **kwargs) -> None:
        self._file = file
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file={self._file})"


class CSVDestination(FileDestination):
    def load(self, data: DataFrame) -> None:
        data.to_csv(self._file, **self._kwargs)


class XLSXDestination(FileDestination):
    def load(self, data: DataFrame) -> None:
        data.to_excel(self._file, **self._kwargs)


class CSVAppendDestination(FileDestination):
    def load(self, data: DataFrame) -> None:
        if os.path.isfile(self._file):
            data.to_csv(self._file, mode="a", header=False, **self._kwargs)
            return
        data.to_csv(self._file, mode="w", header=True, **self._kwargs)
