from extralo.transformer import Transformer
from extralo.typing import DataFrame


class NullTransformer(Transformer):
    def transform(self, data: DataFrame) -> DataFrame:
        return data
