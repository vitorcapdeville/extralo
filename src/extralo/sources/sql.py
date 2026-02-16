from typing import Any, Optional

import pandas as pd
from sqlalchemy import BindParameter


class SQLSource:
    """A class representing a SQL data source.

    Requires sqlparse and sqlalchemy to be installed. The SQL query is executed using the provided database engine.
    The query can have multiple statements, separated by semicolons. The last one must return some data.

    Args:
        engine (object): The database engine object.
        query (str): The SQL query to execute.
        params (dict, optional): The parameters to be passed to the SQL query. Defaults to None.
    """

    def __init__(self, engine: Any, query: str, params: Optional[dict[str, Any]] = None) -> None:
        try:
            import sqlalchemy  # type: ignore # noqa: F401, PLC0415
            import sqlalchemy.exc  # type: ignore # noqa: F401, PLC0415
        except ImportError as err:
            raise ImportError(
                "SQLAlchemy is required to use SQLSource. Please install it with `pip install sqlalchemy`."
            ) from err
        try:
            import sqlparse as sp  # type: ignore # noqa: F401, PLC0415
        except ImportError as err:
            raise ImportError(
                "sqlparse is required to use SQLSource. Please install it with `pip install sqlparse`."
            ) from err
        self._engine = engine
        self._query = query
        self._params = params or {}

    def _prepare_query(self, query_str: str) -> Any:
        """Prepare a SQL query, adding expanding bind parameters for list values.

        Args:
            query_str (str): The SQL query string.

        Returns:
            TextClause: The prepared SQLAlchemy text clause.
        """
        import sqlalchemy as sa  # noqa: PLC0415

        text_query = sa.text(query_str)
        expanding_params: list[BindParameter[Any]] = [
            sa.bindparam(key, expanding=True) for key, value in self._params.items() if isinstance(value, list)
        ]
        if expanding_params:
            text_query = text_query.bindparams(*expanding_params)
        return text_query

    def extract(self) -> pd.DataFrame:
        """Extracts data from the database using the provided SQL query.

        Returns:
            DataFrame: The extracted data as a pandas DataFrame.
        """
        import sqlalchemy.exc as sa_exc  # noqa: PLC0415
        import sqlparse as sp  # type: ignore  # noqa: PLC0415

        query = sp.split(self._query)  # type: ignore
        with self._engine.begin() as connection:
            for statement in query[:-1]:
                connection.execute(
                    self._prepare_query(statement), parameters=self._params, execution_options={"no_parameters": True}
                )
            try:
                data = pd.read_sql(self._prepare_query(query[-1]), connection, params=self._params)  # type: ignore
            except sa_exc.ProgrammingError:
                data = pd.read_sql(self._prepare_query(query[-1]), connection)  # type: ignore

        return data

    def __repr__(self) -> str:
        return f"SQLSource(engine={self._engine})"
