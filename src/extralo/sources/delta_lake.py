from typing import Any, Optional

import pandas as pd

from extralo.source import Source


class DeltaLakeSource(Source):
    """A source class for extracting data from Delta Lake tables.

    Args:
        table_uri (str): The URI of the Delta Lake table.
        partitions (Optional[list[tuple[str]]], optional): List of partition columns to filter the data.
            Defaults to None.
    """

    def __init__(
        self,
        table_uri: str,
        partitions: Optional[list[tuple[str]]] = None,
        **kwargs: Any,
    ) -> None:
        try:
            import deltalake  # noqa: F401
        except ImportError as err:
            raise ImportError(
                "DeltaLake is required to use DeltaLakeSource. Please install it with `pip install deltalake`."
            ) from err
        self._table_uri = table_uri
        self._partitions = partitions
        self._kwargs = kwargs

    def extract(self) -> pd.DataFrame:
        """Extracts data from a Delta Lake table and returns it as a pandas DataFrame.

        Returns:
            DataFrame: The extracted data as a pandas DataFrame.
        """
        import deltalake as dl

        return dl.DeltaTable(self._table_uri).to_pandas(partitions=self._partitions, **self._kwargs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(table_uri={self._table_uri})"
