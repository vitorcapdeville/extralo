from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandera as pa

from extralo.typing import DataFrame


@dataclass
class DataOutput:
    name: str
    data: DataFrame
    schema: type[pa.DataFrameModel] = pa.DataFrameModel

    def validate(self) -> DataFrame:
        return self.schema.validate(self.data, lazy=True)


class Transformer(ABC):
    @abstractmethod
    def transform(self, data: list[DataOutput]) -> list[DataOutput]:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()
