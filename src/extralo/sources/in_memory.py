"""In-memory source that returns data passed as parameter.

Useful when data comes from previous task outputs via inter-task
parameter passing (e.g., ``{{tasks.task_name.output_name}}``).
"""

from typing import TypeVar

T = TypeVar("T")


class InMemorySource:
    """A source that returns pre-loaded data without performing any I/O.

    Complements :class:`~extralo.destinations.InMemoryDestination` for
    building pipelines where data flows between tasks in memory.

    Args:
        data: The data to return when ``extract()`` is called.
    """

    def __init__(self, data: T) -> None:
        self.data = data

    def extract(self) -> T:  # type: ignore[type-var]
        """Return the data passed at construction time.

        Returns:
            The same data, unchanged.
        """
        return self.data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(data=<{type(self.data).__name__}>)"
