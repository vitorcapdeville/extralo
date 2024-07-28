import logging

from extralo.destination import Destination
from extralo.source import Source
from extralo.transformer import Transformer
from extralo.transformers import NullTransformer
from extralo.typing import DataFrame


class ETL:
    def __init__(
        self, source: Source, destinations: list[Destination], transformer: Transformer = NullTransformer()
    ) -> None:
        self._source = source
        self._destinations = destinations
        self._transformer = transformer
        self._logger = logging.getLogger("etl")

    def execute(self) -> DataFrame:
        data = self.extract()
        data = self.transform(data)
        data = self.load(data)

    def extract(self) -> DataFrame:
        self._logger.info(f"Starting extraction for {self._source}")
        data = self._source.extract()
        self._logger.info(f"Extracted {len(data)} records from {self._source}")
        return data

    def transform(self, data) -> DataFrame:
        data = self._transformer.transform(data)
        self._logger.info(f"Tranformed {len(data)} records with {self._transformer}")
        return data

    def load(self, data) -> DataFrame:
        for destination in self._destinations:
            destination.load(data)
            self._logger.info(f"Loaded {len(data)} records into {destination}")
