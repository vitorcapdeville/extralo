from typing import TypedDict

from extralo.transformer import Transformer
from extralo.typing import DataFrame


class NullData(TypedDict):
    data: DataFrame


class NullTransformer(Transformer):
    def transform(self, data: DataFrame) -> NullData:
        return NullData(data=data)
