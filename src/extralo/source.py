from typing import Generic, Protocol, TypeVar

T_co = TypeVar("T_co", covariant=True)


class Source(Generic[T_co], Protocol):
    """Generic protocol for a source that extracts data from somewhere."""

    def extract(self) -> T_co:
        """Extracts data from a source and returns it.

        Returns:
            T_co: The extracted data.
        """
        raise NotImplementedError
