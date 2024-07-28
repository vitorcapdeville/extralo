from unittest.mock import MagicMock, Mock

import pandas as pd
import pandera as pa
from extralo.etl import ETL


def test_etl_delegates_to_components():
    source = Mock()
    data = MagicMock(spec=pd.DataFrame)
    source.extract = Mock(return_value=data)
    trans = Mock()
    trans.transform = Mock(return_value=data)
    dest = Mock()
    dest.load = Mock(return_value=data)
    before_schema = MagicMock(sepc=pa.DataFrameModel)
    before_schema.validate = Mock(return_value=data)
    after_schema = MagicMock(sepc=pa.DataFrameModel)
    after_schema.validate = Mock(return_value=data)

    etl = ETL(
        source=source, transformer=trans, destinations=[dest], before_schema=before_schema, after_schema=after_schema
    )

    etl.execute()

    source.extract.assert_called_once()
    before_schema.validate.assert_called_once()
    trans.transform.assert_called_once_with(data)
    after_schema.validate.assert_called_once()
    dest.load.assert_called_once_with(data)
