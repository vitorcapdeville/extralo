from abc import ABC, abstractmethod

from extralo.typing import DataFrame


class Transformer(ABC):
    @abstractmethod
    def transform(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()
