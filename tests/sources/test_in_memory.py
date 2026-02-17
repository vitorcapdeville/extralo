"""Tests for InMemorySource."""

import pandas as pd

from extralo.sources import InMemorySource


class TestInMemorySource:
    """Tests for the InMemorySource class."""

    def test_extract_returns_list(self):
        """InMemorySource returns the list passed to it."""
        data = [{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}]
        source = InMemorySource(data=data)
        result = source.extract()
        assert result is data

    def test_extract_returns_dict(self):
        """InMemorySource returns a dict passed to it."""
        data = {"key": "value", "nested": {"a": 1}}
        source = InMemorySource(data=data)
        result = source.extract()
        assert result is data

    def test_extract_returns_dataframe(self):
        """InMemorySource works with pandas DataFrames."""
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        source = InMemorySource(data=df)
        result = source.extract()
        assert result is df

    def test_extract_returns_none(self):
        """InMemorySource handles None data."""
        source = InMemorySource(data=None)
        result = source.extract()
        assert result is None

    def test_extract_returns_scalar(self):
        """InMemorySource handles scalar values."""
        source = InMemorySource(data=42)
        result = source.extract()
        assert result == 42

    def test_repr(self):
        """Repr shows data type information."""
        source = InMemorySource(data=[1, 2, 3])
        assert "InMemorySource" in repr(source)
        assert "list" in repr(source)
