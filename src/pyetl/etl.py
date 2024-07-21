import pandas as pd

from pyetl.destinations import Destination
from pyetl.sources import Source
from logging import Logger

class ETL:
    def __init__(self, source: Source, destination: Destination, logger: Logger) -> None:
        self._source = source
        self._destination = destination
        self._logger = logger

    def execute(self):
        data = self.extract()
        self._logger.info(f"Extracted {len(data)} records")
        data = self.transform(data)
        self.load(data)

    def extract(self) -> pd.DataFrame:
        return self._source.extract()

    def transform(self, data) -> pd.DataFrame:
        return data

    def load(self, data) -> pd.DataFrame:
        return self._destination.load(data)
