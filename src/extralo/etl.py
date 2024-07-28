import logging
from typing import Optional

import pandera as pa

from extralo.destination import Destination
from extralo.source import Source
from extralo.transformer import Transformer
from extralo.transformers import NullTransformer
from extralo.typing import DataFrame


class ETL:
    def __init__(
        self,
        sources: dict[str, Source],
        destinations: dict[str, list[Destination]],
        transformer: Transformer = NullTransformer(),
        before_schemas: Optional[dict[str, type[pa.DataFrameModel]]] = None,
        after_schemas: Optional[dict[str, type[pa.DataFrameModel]]] = None,
    ) -> None:
        self._sources = sources
        self._destinations = destinations
        self._transformer = transformer
        self._before_schemas = before_schemas
        self._after_schemas = after_schemas

    def execute(self) -> DataFrame:
        data = self.extract()
        data = self.before_validate(data)
        data = self.transform(data)
        data = self.after_validate(data)
        self.load(data)

    def extract(self) -> dict[str, DataFrame]:
        data = {}
        logger = logging.getLogger("etl")
        for name, source in self._sources.items():
            logger.info(f"Starting extraction for {source}")
            data[name] = source.extract()
            logger.info(f"Extracted {len(data[name])} records from {source}")
        return data

    def before_validate(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        if self._before_schemas is None:
            return data
        return {name: schema.validate(data[name], lazy=True) for name, schema in self._before_schemas.items()}

    def transform(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        data = self._transformer.transform(**data)
        logging.getLogger("etl").info(f"Tranformed data with {self._transformer}")
        return data

    def after_validate(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        if self._after_schemas is None:
            return data
        return {name: schema.validate(data[name], lazy=True) for name, schema in self._after_schemas.items()}

    def load(self, data: dict[str, DataFrame]) -> None:
        logger = logging.getLogger("etl")
        for name, destinations in self._destinations.items():
            for destination in destinations:
                destination.load(data[name])
                logger.info(f"Loaded {len(data[name])} records to {destination}")
