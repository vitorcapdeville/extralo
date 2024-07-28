import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from extralo.typing import DataFrame


class Source(ABC):
    @abstractmethod
    def extract(self) -> DataFrame:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()


@dataclass
class SourceDefinition:
    name: str
    source: Source

    def extract(self) -> DataFrame:
        logger = logging.getLogger("etl")
        logger.info(f"Starting extraction for {self.source}")
        data = self.source.extract()
        logger.info(f"Extracted {len(data)} records from {self.source}")
        return data
