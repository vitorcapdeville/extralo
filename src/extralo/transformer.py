from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandera as pa

from extralo.typing import DataFrame


@dataclass
class DataOutput:
    name: str
    data: DataFrame

    def validate(self, schema: type[pa.DataFrameModel]) -> DataFrame:
        return schema.validate(self.data, lazy=True)


class Transformer(ABC):
    @abstractmethod
    def transform(self, data: list[DataOutput]) -> list[DataOutput]:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()
