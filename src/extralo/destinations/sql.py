from typing import Any
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


class SQLAppendDestination(SQLDestination):
    def __init__(self, engine: sa.Engine, table: str, schema: str, group_column: str, group_value: Any) -> None:
        super().__init__(engine, table, schema, "append")
        self._group_column = group_column
        self._group_value = group_value

    def load(self, data: DataFrame) -> DataFrame:
        insp = sa.inspect(self._engine)
        if insp.has_table(self._table, self._schema):
            metadata_obj = sa.MetaData()
            table = sa.Table(self._table, metadata_obj, autoload_with=self._engine, schema=self._schema)
            if self._group_column not in table.c:
                raise KeyError(f"Column '{self._group_column}' not found in table '{self._table}'")
            stmt = sa.delete(table).where(table.c[self._group_column] == self._group_value)
            with self._engine.begin() as conn:
                conn.execute(stmt)

        return super().load(data)
