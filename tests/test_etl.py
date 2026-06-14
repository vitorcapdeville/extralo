from unittest.mock import MagicMock

import pandas as pd
import pytest

from extralo.etl import ETL, ETLStepError, IncompatibleStepsError


@pytest.fixture
def mock_data():
    return MagicMock(spec=pd.DataFrame)


@pytest.fixture
def mock_source(mock_data):
    class SourceStub:
        def extract(self):  # noqa: PLR6301
            return mock_data

    return SourceStub()


def mock_transform(source):
    return {"source_trans": source["source"]}


@pytest.fixture
def mock_dest():
    class DestStub:
        def load(self, data):  # noqa: PLR6301
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


def test_extract_wraps_source_failure_with_step_context(mock_dest):
    class FailingSource:
        def extract(self):  # noqa: PLR6301
            raise ValueError("missing file")

        def __repr__(self):
            return "FailingSource(file=missing.csv)"

    etl = ETL(
        sources={"input": FailingSource()},
        destinations={"input": [mock_dest]},
    )

    with pytest.raises(ETLStepError) as exc_info:
        etl.extract()

    assert exc_info.value.step == "extract"
    assert exc_info.value.name == "input"
    assert repr(exc_info.value.component) == "FailingSource(file=missing.csv)"
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_transform_wraps_failure_with_step_context(mock_source, mock_dest, mock_data):
    def failing_transform(source):
        raise ValueError("invalid input")

    etl = ETL(
        sources={"source": mock_source},
        transformer=failing_transform,
        destinations={"source": [mock_dest]},
    )

    with pytest.raises(ETLStepError) as exc_info:
        etl.transform({"source": mock_data})

    assert exc_info.value.step == "transform"
    assert exc_info.value.name == "failing_transform"
    assert exc_info.value.component is failing_transform
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_load_wraps_destination_failure_with_step_context(mock_data):
    class FailingDestination:
        def load(self, data):  # noqa: PLR6301
            raise RuntimeError("cannot write")

        def __repr__(self):
            return "FailingDestination(table=output)"

    etl = ETL(
        sources={},
        destinations={"output": [FailingDestination()]},
    )

    with pytest.raises(ETLStepError) as exc_info:
        etl.load({"output": mock_data})

    assert exc_info.value.step == "load"
    assert exc_info.value.name == "output"
    assert repr(exc_info.value.component) == "FailingDestination(table=output)"
    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_execute_preserves_step_error_cause(mock_dest):
    class FailingSource:
        def extract(self):  # noqa: PLR6301
            raise ValueError("missing file")

    etl = ETL(
        sources={"input": FailingSource()},
        destinations={"input": [mock_dest]},
    )

    with pytest.raises(ETLStepError) as exc_info:
        etl.execute()

    assert exc_info.value.step == "extract"
    assert exc_info.value.name == "input"
    assert isinstance(exc_info.value.__cause__, ValueError)
