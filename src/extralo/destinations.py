from abc import ABC, abstractmethod

import pandas as pd
import sqlalchemy as sa


class Destination(ABC):
    @abstractmethod
    def load(self, data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()


class MemoryDestination(Destination):
    def load(self, data: pd.DataFrame) -> pd.DataFrame:
        return data


class SQLDestination(Destination):
    def __init__(self, engine: sa.Engine, table: str, schema: str, if_exists: str) -> None:
        self._engine = engine
        self._table = table
        self._if_exists = if_exists
        self._schema = schema

    def load(self, data: pd.DataFrame) -> pd.DataFrame:
        data.to_sql(name=self._table, schema=self._schema, con=self._engine, if_exists=self._if_exists, index=False)
        return data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(table={self._table}, schema={self._schema}, if_exists={self._if_exists})"
