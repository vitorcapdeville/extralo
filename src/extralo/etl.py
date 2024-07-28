import logging

from extralo.destination import DestinationDefinition
from extralo.source import SourceDefinition
from extralo.transformer import DataOutput, Transformer
from extralo.transformers import NullTransformer
from extralo.typing import DataFrame


class ETL:
    def __init__(
        self,
        sources: list[SourceDefinition],
        destinations: list[DestinationDefinition],
        transformer: Transformer = NullTransformer(),
    ) -> None:
        self._sources = sources
        self._destinations = destinations
        self._transformer = transformer

    def execute(self) -> DataFrame:
        data = self.extract()
        data = self.validate(data)
        data = self.transform(data)
        data = self.validate(data)
        self.load(data)

    def extract(self) -> list[DataOutput]:
        data = [DataOutput(name=source.name, data=source.extract()) for source in self._sources]
        return data

    def validate(self, data: list[DataOutput]) -> list[DataOutput]:
        return [DataOutput(name=output.name, data=output.validate(), schema=output.schema) for output in data]

    def transform(self, data: list[DataOutput]) -> list[DataOutput]:
        data = self._transformer.transform(data)
        logging.getLogger("etl").info(f"Tranformed data with {self._transformer}")
        return data

    def load(self, data: list[DataOutput]) -> None:
        destinations = self._destinations.copy()
        for data_output in data:
            selected_destination = None
            for destination in destinations:
                if data_output.name == destination.name:
                    selected_destination = destination
                    break
            if selected_destination is None:
                raise ValueError(f"Destination not found for {data_output.name}")
            selected_destination.load(data_output.data)
            destinations.remove(selected_destination)
