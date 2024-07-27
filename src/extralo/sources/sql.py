import pandas as pd
import sqlalchemy as sa

from extralo.source import Source
from extralo.typing import DataFrame


class SQLSource(Source):
    def __init__(self, engine, query, params={}) -> None:
        self._engine = engine
        self._query = query
        self._params = params

    def extract(self) -> DataFrame:
        with self._engine.connect() as connection:
            return pd.read_sql(sa.text(self._query), connection, params=self._params)
