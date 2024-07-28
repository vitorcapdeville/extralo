import pandas as pd
from extralo.destinations.memory import MemoryDestination


def test_memory_destination_load():
    destination = MemoryDestination()
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    loaded_data = destination.load(data)

    assert loaded_data.equals(data)
