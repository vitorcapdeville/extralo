from unittest.mock import MagicMock

import pandas as pd
import pandera as pa
from extralo.destination import Destination
from extralo.models import TolerableDataFrameModel
from pandas.testing import assert_frame_equal


def test_tolerable_data_frame_model_calls_destination_load_on_fail():
    destination = MagicMock(spec=Destination)

    class TestModel(TolerableDataFrameModel):
        class Config:
            failure_id = "teste_id"
            failure_destination = destination

        field: int = pa.Field(ge=0)

    data = pd.DataFrame({"field": [-1]})

    validated_data = TestModel.validate(data, lazy=True)

    assert_frame_equal(validated_data, data)

    destination.load.assert_called_once()


def test_tolerable_data_frame_model_does_not_call_destination_load_on_success():
    destination = MagicMock(spec=Destination)

    class TestModel(TolerableDataFrameModel):
        class Config:
            failure_id = "teste_id"
            failure_destination = destination

        field: int = pa.Field(ge=0)

    data = pd.DataFrame({"field": [2]})

    validated_data = TestModel.validate(data, lazy=True)

    assert_frame_equal(validated_data, data)

    destination.load.assert_not_called()
