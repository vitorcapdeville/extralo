from typing import Any, Literal, Optional, Union

from extralo.destination import Destination
from extralo.typing import DataFrame


class DeltaLakeDestination(Destination):
    """A destination class for saving data to a Delta Lake table.

    Args:
        table_uri (str): The path to the Delta Lake table.
        **kwargs: Additional keyword arguments to be passed to the save function.
    """

    def __init__(
        self,
        table_uri: str,
        mode: Literal["error", "append", "overwrite", "ignore"],
        partition_by: Optional[Union[list[str], str]],
        **kwargs: Any,
    ) -> None:
        try:
            import deltalake  # noqa: F401
        except ImportError as err:
            raise ImportError(
                "DeltaLake is required to use DeltaLakeDestination. Please install it with `pip install deltalake`."
            ) from err
        self._table_uri = table_uri
        self._mode = mode
        self._partition_by = partition_by
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(table={self._table_uri}, mode={self._mode})"

    def load(self, data: DataFrame) -> None:
        """Loads the given DataFrame into the Delta Lake table.

        Args:
            data (DataFrame): The DataFrame to be loaded.
        """
        import deltalake as dl

        dl.write_deltalake(
            table_or_uri=self._table_uri, data=data, mode=self._mode, partition_by=self._partition_by, **self._kwargs
        )
