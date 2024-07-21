from typing import Protocol

import pandas as pd
import sqlalchemy as sa


class Source(Protocol):
    def extract(self) -> pd.DataFrame:
        pass


class SQLSource:
    def __init__(self, engine, query, params={}) -> None:
        self._engine = engine
        self._query = query
        self._params = params

    def extract(self):
        with self._engine.connect() as connection:
            return pd.read_sql(sa.text(self._query), connection, params=self._params)


class FileSource:
    def __init__(self, file: str) -> None:
        self._file = file


class CSVSource(FileSource):
    def extract(self) -> pd.DataFrame:
        return pd.read_csv(self._file)


class XLSXSource(FileSource):
    def extract(self) -> pd.DataFrame:
        return pd.read_excel(self._file)
