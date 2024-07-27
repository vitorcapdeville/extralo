from extralo.destination import Destination
from extralo.typing import DataFrame


class MemoryDestination(Destination):
    def load(self, data: DataFrame) -> DataFrame:
        return data
