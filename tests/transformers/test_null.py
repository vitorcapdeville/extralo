from extralo.transformers.null import NullTransformer
import pandas as pd


def test_null_transformer_transforms_data():
    transformer = NullTransformer()
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    transformed_data = transformer.transform(data)

    assert transformed_data.equals(data)
