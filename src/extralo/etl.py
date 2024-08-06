import inspect
import logging
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Optional, _TypedDictMeta, get_type_hints  # type: ignore

from extralo.destination import Destination
from extralo.source import Source
from extralo.transformer import Transformer
from extralo.typing import DataFrame, DataFrameModel


class IncompatibleStepsError(Exception):
    """Exception raised when two steps in a process are incompatible.

    Attributes:
        step_base (str): The name of the base step.
        keys_base (set[str]): The keys associated with the base step.
        step (str): The name of the incompatible step.
        keys (set[str]): The keys associated with the incompatible step.
    """

    def __init__(self, step_base: str, keys_base: set[str], step: str, keys: set[str]) -> None:
        self.step_base = step_base
        self.step = step
        self.keys_base = keys_base
        self.keys = keys

    def __str__(self) -> str:
        return (
            f"Step '{self.step}' with keys {self.keys} is incompatible with step '{self.step_base}' "
            f"with keys {self.keys_base}"
        )


def _validate_steps(step1_keys: set[str], step1_name: str, step2_keys: set[str], step2_name: str) -> None:
    if step1_keys != step2_keys:
        raise IncompatibleStepsError(step1_name, step1_keys, step2_name, step2_keys)


class ETL:
    """ETL - Extract, Load and Transform data from sources to destinations.

    The ETL class provide functionality around the extract, load and transform operations.
    The main functionalities are:

    - Ensure that the process will be executed in the correct order, and the data will only be loaded if satisfies the
        provided schema.
    - Allow the use of multiple sources.
    - Allow the use of multiple destinations for a single data.
    - Allow the use of different destinations for different data.
    - Provides configurable logging for each step of the process.
    - Run I/O operations in parallel, using threads.
    - Explicitly define where the data is comming from, what is happening with it and where it is going.

    The pipeline relies on dictionaries to define sources, validators and destinations. The keys of the dictionaries are
    used to match the data between the steps.

    In the earlier steps, it's possible to validate the match of the keys between the steps, to ensure that the data is
    being processed correctly.

    However, in the transform step, it's not possible to validated the keys, since the transformer can change the keys
    of the data. In this case, the validation will be done only at runtime.

    This can be resolved using return type annotations and the `TypedDict` from `typing` module. If the transformer
    return a `TypedDict`, the keys will be validated at initialization.

    Args:
        sources (dict[str, Source]): A dictionary with the sources to extract data from.
        destinations (dict[str, list[Destination]]): A dictionary with the destinations to load data to.
            Each value must be a list of destinations, and the data with that key will be loaded to all the
            destionations provided in the list.
        transformer (Transformer, optional): A transformer to transform the data. No transformation is done by default.
        before_schemas (Optional[dict[str, type[pa.DataFrameModel]]], optional): A dictionary with the schemas to
            validate the data before the transformation. Defaults to None.
        after_schemas (Optional[dict[str, type[pa.DataFrameModel]]], optional): A dictionary with the schemas to
            validate the data after the transformation. Defaults to None.
    """

    def __init__(
        self,
        sources: dict[str, Source],
        destinations: dict[str, list[Destination]],
        transformer: Optional[Transformer] = None,
        before_schemas: Optional[dict[str, DataFrameModel]] = None,
        after_schemas: Optional[dict[str, DataFrameModel]] = None,
    ) -> None:
        self._sources = sources
        self._destinations = destinations
        self._transformer = transformer
        self._before_schemas = before_schemas
        self._after_schemas = after_schemas

        if self._before_schemas is not None:
            _validate_steps(set(self._sources.keys()), "extract", set(self._before_schemas.keys()), "before_schema")

        if self._transformer is None:
            return

        trasnform_args = inspect.getargs(self._transformer.transform.__code__).args
        _validate_steps(set(self._sources.keys()), "extract", set(trasnform_args[1:]), "transform")

        transform_output = get_type_hints(self._transformer.transform).get("return", None)
        if transform_output is None or not isinstance(transform_output, _TypedDictMeta):
            logging.getLogger("etl").warning(
                "Transformer output type hints are not a TypedDict, validation will be done only at runtime."
            )
            return

        transform_output_dict = transform_output.__annotations__
        if self._after_schemas is not None:
            _validate_steps(
                set(transform_output_dict.keys()), "transform", set(self._after_schemas.keys()), "after_schema"
            )
        _validate_steps(set(transform_output_dict.keys()), "transform", set(self._destinations.keys()), "load")

    def execute(self) -> None:
        """Execute the ETL process.

        Extract the data from the sources, validate it against the before schemas, transform it, validate it against
        the after schemas and load it to the destinations.

        Returns:
            This method does not return anything, and it's used for it's only side effect: load the data to the
                destinations.
        """
        data = self.extract()
        data = self.before_validate(data)
        data = self.transform(data)
        data = self.after_validate(data)
        self.load(data)

    def extract(self) -> dict[str, DataFrame]:
        """Extract the data from the provided sources and load it into a dictionary with same keys as the sources.

        The extraction is done in parallel, using threads.
        This method use the `etl` logger to log the extraction process, which can be customized by the user.

        Returns:
            dict[str, DataFrame]: A dictionary with the data extracted from the sources.
        """
        data = {}
        futures = {}
        logger = logging.getLogger("etl")

        with ThreadPoolExecutor(max_workers=5) as executor:
            for name, source in self._sources.items():
                logger.info(f"Starting extraction for {source}")
                future: Future[DataFrame] = executor.submit(source.extract)
                futures[future] = name

            for future in as_completed(futures):
                name = futures[future]
                data[name] = future.result()
                logger.info(f"Extracted {len(data[name])} records from {source}")

        return data

    def before_validate(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Validate the extracted data against the provided schemas.

        If the before_schemas was not provided, no validation will be done and the data will be returned as is.

        Args:
            data (dict[str, DataFrame]): The data to be validated.

        Returns:
            dict[str, DataFrame]: A dictionary with the validated data (or the original data if no schema was provided).
                Uses the same keys as the input data.
        """
        if self._before_schemas is None:
            return data

        return {name: schema.validate(data[name], lazy=True) for name, schema in self._before_schemas.items()}

    def transform(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Transform the data extracted from the source according to the `Transformer` class provided.

        This method use the `etl` logger to log the extraction process, which can be customized by the user.
        The data from the sources will be passes to the `transform` method of the `Transformer` as keyword arguments.

        Args:
            data (dict[str, DataFrame]): The data to be transformed.

        Returns:
            dict[str, DataFrame]: A dictionary with the transformed data. The keys could be different from the
                input data.
        """
        if self._transformer is None:
            logging.getLogger("etl").info("Skipping transform step since no Transformer was specified.")
            return data

        data = self._transformer.transform(**data)
        logging.getLogger("etl").info(f"Tranformed data with {self._transformer}")
        return data

    def after_validate(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Validate the transformed data against the provided schemas.

        If the after_schemas was not provided, no validation will be done and the data will be returned as is.

        Args:
            data (dict[str, DataFrame]): The data to be validated.

        Returns:
            dict[str, DataFrame]: A dictionary with the validated data (or the original data if no schema was provided).
                Uses the same keys as the input data.
        """
        if self._after_schemas is None:
            return data

        _validate_steps(set(data.keys()), "transform", set(self._after_schemas.keys()), "after_schema")

        return {name: schema.validate(data[name], lazy=True) for name, schema in self._after_schemas.items()}

    def load(self, data: dict[str, DataFrame]) -> None:
        """Load the data to the provided destinations.

        The data will be loaded in parallel, using threads.

        Args:
            data (dict[str, DataFrame]): The data to be loaded. The keys must match the keys of the destinations.
        """
        logger = logging.getLogger("etl")

        _validate_steps(set(data.keys()), "transform", set(self._destinations.keys()), "load")

        futures = {}

        with ThreadPoolExecutor(max_workers=5) as executor:
            for name, destinations in self._destinations.items():
                data_to_load = data[name]
                size = len(data_to_load)
                for destination in destinations:
                    logger.info(f"Starting load of {size} records to {destination}")
                    future = executor.submit(destination.load, data_to_load)
                    futures[future] = (size, destination)

            for future in as_completed(futures):
                size, dest = futures[future]
                future.result()
                logger.info(f"Loaded {size} records to {dest}")
