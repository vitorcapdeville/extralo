from extralo.transformers.null import NullTransformer
import pandas as pd
from pandas.testing import assert_frame_equal


def test_null_transformer_transforms_data():
    transformer = NullTransformer()
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    transformed_data = transformer.transform(data)

    assert_frame_equal(transformed_data["data"], data)
