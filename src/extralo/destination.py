from typing import Generic, Protocol, TypeVar

T_contra = TypeVar("T_contra", contravariant=True)


class Destination(Generic[T_contra], Protocol):
    """Generic protocol for a destination that loads data to somewhere."""

    def load(self, data: T_contra) -> None:
        """Load the given data into the destination.

        Args:
            data (T_contra): The data to be loaded into the destination.
        """
        ...
