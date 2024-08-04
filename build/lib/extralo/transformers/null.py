from extralo.transformer import Transformer
from extralo.typing import DataFrame


class NullTransformer(Transformer):
    """Transformer that does nothing to the data.

    Used when no transformation is needed.
    """

    def transform(self, **kwargs: DataFrame) -> dict[str, DataFrame]:
        """Return the input dataframes unchanged.

        Args:
            kwargs: A dictionary of input dataframes.

        Returns:
            A dictionary containing the transformed dataframes.
        """
        return kwargs
