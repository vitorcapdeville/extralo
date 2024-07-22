from abc import ABC, abstractmethod


class Transformer(ABC):
    @abstractmethod
    def transform(self, data):
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()


class NullTransformer(Transformer):
    def transform(self, data):
        return data
