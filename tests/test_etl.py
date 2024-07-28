from extralo.etl import ETL
from unittest.mock import Mock
import pandas as pd


def test_etl_delegates_to_components_and_returns_data():
    source = Mock()
    data = pd.DataFrame()
    source.extract = Mock(return_value=data)
    trans = Mock()
    trans.transform = Mock(return_value=data)
    dest = Mock()
    dest.load = Mock(return_value=data)

    etl = ETL(source=source, transformer=trans, destination=dest)

    assert etl.execute() is data

    source.extract.assert_called_once()
    trans.transform.assert_called_once_with(data)
    dest.load.assert_called_once_with(data)
