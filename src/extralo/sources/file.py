from abc import ABC

import pandas as pd

from extralo.source import Source
from extralo.typing import DataFrame


class FileSource(Source, ABC):
    def __init__(self, file: str, **kwargs) -> None:
        self._file = file
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file={self._file})"


class CSVSource(FileSource):
    def extract(self) -> DataFrame:
        return pd.read_csv(self._file, **self._kwargs)


class XLSXSource(FileSource):
    def extract(self) -> DataFrame:
        return pd.read_excel(self._file, **self._kwargs)
