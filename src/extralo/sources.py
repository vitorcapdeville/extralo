from abc import ABC, abstractmethod

import pandas as pd
import sqlalchemy as sa


class Source(ABC):
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()


class SQLSource(Source):
    def __init__(self, engine, query, params={}) -> None:
        self._engine = engine
        self._query = query
        self._params = params

    def extract(self):
        with self._engine.connect() as connection:
            return pd.read_sql(sa.text(self._query), connection, params=self._params)


class FileSource(Source, ABC):
    def __init__(self, file: str, **kwargs) -> None:
        self._file = file
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file={self._file})"


class CSVSource(FileSource):
    def extract(self) -> pd.DataFrame:
        return pd.read_csv(self._file, **self._kwargs)


class XLSXSource(FileSource):
    def extract(self) -> pd.DataFrame:
        return pd.read_excel(self._file, **self._kwargs)
