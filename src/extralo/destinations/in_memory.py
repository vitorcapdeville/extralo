"""In-memory destination that simply returns data without persisting it.

Useful for workflows that need to pass data between tasks without
writing to an external storage system.
"""

from typing import TypeVar

T = TypeVar("T")


class InMemoryDestination:
    """A destination that does not persist data anywhere.

    It satisfies the ``Destination`` protocol by accepting data
    in ``load()`` and returning it unchanged.  This is handy when
    the ETL output is only needed as an intermediate result for
    downstream tasks.
    """

    def load(self, data: T) -> T:  # noqa: PLR6301
        """Return data as-is without any I/O.

        Args:
            data: The data to "load".

        Returns:
            The same data, unchanged.
        """
        return data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
