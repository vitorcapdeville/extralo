import sqlalchemy as sa

from extralo.destination import Destination
from extralo.typing import DataFrame


class SQLDestination(Destination):
    def __init__(self, engine: sa.Engine, table: str, schema: str, if_exists: str) -> None:
        self._engine = engine
        self._table = table
        self._if_exists = if_exists
        self._schema = schema

    def load(self, data: DataFrame) -> DataFrame:
        data.to_sql(name=self._table, schema=self._schema, con=self._engine, if_exists=self._if_exists, index=False)
        return data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(table={self._table}, schema={self._schema}, if_exists={self._if_exists})"
