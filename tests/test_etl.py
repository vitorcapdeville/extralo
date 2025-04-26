from unittest.mock import MagicMock

import pandas as pd
import pytest

from extralo.etl import ETL, IncompatibleStepsError


@pytest.fixture
def mock_data():
    return MagicMock(spec=pd.DataFrame)


@pytest.fixture
def mock_source(mock_data):
    class SourceStub:
        def extract(self):
            return mock_data

    return SourceStub()


def mock_transform(source):
    return {"source_trans": source["source"]}


@pytest.fixture
def mock_dest():
    class DestStub:
        def load(self, data):
            return

    return DestStub()


def test_etl_delegates_to_components(mock_source, mock_dest):
    etl = ETL(
        sources={"source": mock_source},
        transformer=mock_transform,
        destinations={"source_trans": [mock_dest]},
    )

    etl.execute()

    assert True


def test_etl_works_without_transformer(mock_source, mock_dest):
    etl = ETL(
        sources={"source": mock_source},
        destinations={"source": [mock_dest]},
    )

    etl.execute()

    assert True


def test_etl_fails_when_transformer_and_destination_are_incompatible(mock_source, mock_dest):
    etl = ETL(
        sources={"source": mock_source},
        transformer=mock_transform,
        destinations={"not_source_trans": [mock_dest]},
    )
    with pytest.raises(IncompatibleStepsError, match="load"):
        etl.execute()


def test_etl_fails_when_source_and_transformer_are_incompatible(mock_source, mock_dest):
    def mock_transform_2(not_source):
        return not_source

    with pytest.raises(IncompatibleStepsError, match="transform"):
        ETL(
            sources={"source": mock_source},
            transformer=mock_transform_2,
            destinations={"source_trans": [mock_dest]},
        )
