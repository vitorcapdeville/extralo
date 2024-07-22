import logging

import pandas as pd

from extralo.destinations import Destination
from extralo.sources import Source
from extralo.transformers import Transformer, NullTransformer


class ETL:
    def __init__(self, source: Source, destination: Destination, transformer: Transformer = NullTransformer()) -> None:
        self._source = source
        self._destination = destination
        self._transformer = transformer
        self._logger = logging.getLogger("etl")

    def execute(self):
        data = self.extract()
        data = self.transform(data)
        self.load(data)

    def extract(self) -> pd.DataFrame:
        self._logger.info(f"Starting extraction for {self._source}")
        data = self._source.extract()
        self._logger.info(f"Extracted {len(data)} records from {self._source}")
        return data

    def transform(self, data) -> pd.DataFrame:
        data = self._transformer.transform(data)
        self._logger.info(f"Tranformed {len(data)} records with {self._transformer}")
        return data

    def load(self, data) -> pd.DataFrame:
        data = self._destination.load(data)
        self._logger.info(f"Loaded {len(data)} records into {self._destination}")
        return data
