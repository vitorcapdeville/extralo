from extralo.etl import ETL
from unittest.mock import Mock
import pandas as pd


def test_etl_delegates_to_components():
    source = Mock()
    data = pd.DataFrame()
    source.extract = Mock(return_value=data)
    trans = Mock()
    trans.transform = Mock(return_value=data)
    dest = Mock()
    dest.load = Mock(return_value=data)

    etl = ETL(source=source, transformer=trans, destinations=[dest])

    etl.execute()

    source.extract.assert_called_once()
    trans.transform.assert_called_once_with(data)
    dest.load.assert_called_once_with(data)
