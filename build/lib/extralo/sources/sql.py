from typing import Any, Optional

from extralo.source import Source
from extralo.typing import DataFrame


class SQLSource(Source):
    """A class representing a SQL data source.

    Args:
        engine (object): The database engine object.
        query (str): The SQL query to execute.
        params (dict, optional): The parameters to be passed to the SQL query. Defaults to None.
    """

    def __init__(self, engine: Any, query: str, params: Optional[dict[str, Any]] = None) -> None:
        try:
            import sqlalchemy  # noqa: F401
        except ImportError as err:
            raise ImportError(
                "SQLAlchemy is required to use SQLSource. Please install it with `pip install sqlalchemy`."
            ) from err
        try:
            import pandas  # noqa: F401
        except ImportError as err:
            raise ImportError(
                "Pandas is required to use SQLSource. Please install it with `pip install pandas`."
            ) from err
        self._engine = engine
        self._query = query
        self._params = params or {}

    def extract(self) -> DataFrame:
        """Extracts data from the database using the provided SQL query.

        Returns:
            DataFrame: The extracted data as a pandas DataFrame.
        """
        import pandas as pd
        import sqlalchemy as sa

        with self._engine.connect() as connection:
            return pd.read_sql(sa.text(self._query), connection, params=self._params)
