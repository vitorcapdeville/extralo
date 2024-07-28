import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from extralo.typing import DataFrame


class Destination(ABC):
    @abstractmethod
    def load(self, data: DataFrame):
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()


@dataclass
class DestinationDefinition:
    name: str
    destinations: list[Destination]

    def load(self, data: DataFrame) -> None:
        logger = logging.getLogger("etl")
        for destination in self.destinations:
            destination.load(data)
            logger.info(f"Loaded {len(data)} records into {destination}")
