from typing import Protocol

import pandas as pd
import sqlalchemy as sa


class Destination(Protocol):
    def load(self, data: pd.DataFrame) -> pd.DataFrame:
        pass


class SQLDestination(Destination):
    def __init__(self, engine: sa.Engine, table: str, if_exists: str) -> None:
        self._engine = engine
        self._table = table
        self._if_exists = if_exists

    def load(self, data: pd.DataFrame) -> pd.DataFrame:
        data.to_sql(self._table, self._engine, if_exists=self._if_exists)
        return data
