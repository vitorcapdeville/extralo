import logging

import pandas as pd

from extralo.destinations import Destination
from extralo.sources import Source


class ETL:
    def __init__(self, source: Source, destination: Destination) -> None:
        self._source = source
        self._destination = destination
        self._logger = logging.getLogger("etl")

    def execute(self):
        data = self.extract()
        data = self.transform(data)
        self.load(data)

    def extract(self) -> pd.DataFrame:
        data = self._source.extract()
        self._logger.info(f"Extracted {len(data)} records from {self._source.__class__.__name__}")
        return data

    def transform(self, data) -> pd.DataFrame:
        self._logger.info("No transformation made.")
        return data

    def load(self, data) -> pd.DataFrame:
        self._logger.info(f"Loaded {len(data)} records into {self._destination.__class__.__name__}")
        return self._destination.load(data)
