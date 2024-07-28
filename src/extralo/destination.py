from abc import ABC, abstractmethod

from extralo.typing import DataFrame


class Destination(ABC):
    @abstractmethod
    def load(self, data: DataFrame):
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()
