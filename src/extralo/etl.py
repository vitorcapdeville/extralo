from __future__ import annotations

import inspect
import warnings
from collections.abc import Callable, Sized
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from functools import partial
from typing import Generic, Optional, TypeVar

import loguru
from loguru import logger

from extralo.destination import Destination
from extralo.source import Source

T = TypeVar("T", bound=Sized)

TransformerFunction = Callable[..., dict[str, T]]


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


class ETLStepError(Exception):
    """Exception raised when a named ETL step fails.

    Attributes:
        step: ETL step that failed (``extract``, ``transform`` or ``load``).
        name: Logical source, transformer or destination/output name.
        component: Source, transformer or destination object that failed.
    """

    def __init__(self, step: str, name: str, component: object) -> None:
        self.step = step
        self.name = name
        self.component = component
        super().__init__(f"ETL step '{step}' failed for '{name}' using {component}")


def _validate_steps(step1_keys: set[str], step1_name: str, step2_keys: set[str], step2_name: str) -> None:
    if step1_keys != step2_keys:
        raise IncompatibleStepsError(step1_name, step1_keys, step2_name, step2_keys)


def _validate_etl(
    sources_keys: set[str],
    transform_method: Optional[TransformerFunction[T]],  # noqa: UP045
) -> None:
    if transform_method is None:
        return

    trasnform_args = inspect.getargs(transform_method.__code__)

    args = set(trasnform_args.args) - {"self", "cls"}

    if trasnform_args.varkw is None:
        _validate_steps(set(sources_keys), "extract", args, "transform")


def _extract(source: Source[T], logger: loguru.Logger) -> T:
    logger.info(f"Starting extraction for {source}")
    data = source.extract()
    logger.info(f"Extracted {len(data)} records from {source}")
    return data


def _load(data: T, destination: Destination[T], logger: loguru.Logger) -> T:
    logger.info(f"Starting load of {len(data)} records to {destination}")
    result = destination.load(data)
    logger.info(f"Loaded {len(data)} records to {destination}")
    return result


class ETL(Generic[T]):
    """ETL - Extract, Load and Transform data from sources to destinations.

    The ETL class provide functionality around the extract, load and transform operations.
    The main functionalities are:

    - Ensure that the process will be executed in the correct order, without the possibility of manual interference.
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

    It's strongly recommended to use Pandera decorators to validate the data in the transform callable.

    Args:
        sources (dict[str, Source]): A dictionary with the sources to extract data from.
        destinations (dict[str, list[Destination]]): A dictionary with the destinations to load data to.
            Each value must be a list of destinations, and the data with that key will be loaded to all the
            destionations provided in the list.
        transformer (Callable[..., dict[str, DataFrame]], optional): A transformer to transform the data.
            No transformation is done by default.
    """

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        sources: dict[str, Source[T]],
        destinations: dict[str, list[Destination[T]]],
        transformer: Optional[TransformerFunction[T]] = None,  # noqa: UP045
        name: Optional[str] = None,  # noqa: UP045
    ) -> None:
        self._logger = logger.bind(etl_name=name, status="pending")

        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")

            _validate_etl(
                set(sources.keys()),
                transformer,
            )
        for warn in warns:
            self._logger.warning(warn.message)

        self._sources = sources
        self._destinations = destinations
        self._transformer = transformer
        self._name = name

    def execute(self) -> dict[str, T]:
        """Execute the ETL process.

        Extract the data from the sources, validate it against the before schemas, transform it, validate it against
        the after schemas and load it to the destinations.

        Returns:
            dict[str, T]: A dictionary mapping destination keys to the loaded data.
                When multiple destinations share the same key, only the first result is kept
                (they all receive the same input data).
        """
        self._logger.info(f"Starting ETL process for {self._name}.", status="running")
        self._logger = self._logger.patch(lambda record: record["extra"].update(status="running"))
        try:
            data = self.extract()

            with warnings.catch_warnings(record=True) as warns:
                warnings.simplefilter("always")
                data = self.transform(data)
            for warn in warns:
                self._logger.warning(warn.message)

            result = self.load(data)
        except Exception as e:
            self._logger.patch(lambda record: record["extra"].update(status="failed")).error(
                f"Failed to execute ETL process for {self._name}: \n {e}"
            )
            raise
        else:
            self._logger.patch(lambda record: record["extra"].update(status="success")).success(
                f"ETL process for {self._name} executed successfully."
            )
            return result

    def extract(self) -> dict[str, T]:
        """Extract the data from the provided sources and load it into a dictionary with same keys as the sources.

        The extraction is done in parallel, using threads.
        This method use the `etl` logger to log the extraction process, which can be customized by the user.

        Returns:
            dict[str, DataFrame]: A dictionary with the data extracted from the sources.

        Raises:
            ETLStepError: If a source fails while extracting data.
        """
        results: dict[str, T] = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures: dict[Future[T], tuple[str, Source[T]]] = {
                executor.submit(_extract, source, self._logger): (name, source)
                for name, source in self._sources.items()
            }
            for future in as_completed(futures):
                name, source = futures[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    raise ETLStepError("extract", name, source) from e

        return {name: results[name] for name in self._sources}

    def transform(self, data: dict[str, T]) -> dict[str, T]:
        """Transform the data extracted from the source according to the `Transformer` class provided.

        This method use the `etl` logger to log the extraction process, which can be customized by the user.
        The data from the sources will be passes to the `transform` method of the `Transformer` as keyword arguments.

        Args:
            data (dict[str, DataFrame]): The data to be transformed.

        Returns:
            dict[str, DataFrame]: A dictionary with the transformed data. The keys could be different from the
                input data.

        Raises:
            ETLStepError: If the transformer fails while processing the data.
        """
        if self._transformer is None:
            self._logger.info("Skipping transform step since no Transformer was specified.")
            return data

        transformer_name = getattr(self._transformer, "__name__", self._transformer.__class__.__name__)
        try:
            data = self._transformer(**data)
        except Exception as e:
            raise ETLStepError("transform", str(transformer_name), self._transformer) from e
        self._logger.info(f"Transformed data with {self._transformer}")

        return data

    def load(self, data: dict[str, T]) -> dict[str, T]:
        """Load the data to the provided destinations.

        The data will be loaded in parallel, using threads.

        Args:
            data (dict[str, DataFrame]): The data to be loaded. The keys must match the keys of the destinations.

        Returns:
            dict[str, T]: A dictionary mapping each destination key to its loaded data.
                When multiple destinations share the same key, only the first result is kept.

        Raises:
            IncompatibleStepsError: If the data keys do not match the destination keys.
            ETLStepError: If a destination fails while loading data.
        """
        data_keys = set(data.keys())
        destination_keys = set(self._destinations.keys())
        if data_keys != destination_keys:
            raise IncompatibleStepsError("transform", data_keys, "load", destination_keys)

        futures: dict[str, list[Future[T]]] = {}
        future_context: dict[Future[T], tuple[str, Destination[T]]] = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            for name, destinations in self._destinations.items():
                data_to_load = data[name]
                futures[name] = []
                for destination in destinations:
                    future = executor.submit(partial(_load, data_to_load, destination, logger=self._logger))
                    futures[name].append(future)
                    future_context[future] = (name, destination)

            results: dict[str, T] = {}
            for name, name_futures in futures.items():
                for future in as_completed(name_futures):
                    try:
                        result = future.result()
                    except Exception as e:  # noqa: PERF203
                        destination_name, destination = future_context[future]
                        raise ETLStepError("load", destination_name, destination) from e
                    else:
                        # Keep only the first result per key (all destinations get the same data)
                        if name not in results:
                            results[name] = result

        return results


class ETLSequentialLoad(ETL[T]):
    """Same as ETL, but loads the data sequentially instead of in parallel."""

    def load(self, data: dict[str, T]) -> dict[str, T]:
        """Load the data to the provided destinations.

        The data will be loaded sequentially.

        Args:
            data (dict[str, DataFrame]): The data to be loaded. The keys must match the keys of the destinations.

        Returns:
            dict[str, T]: A dictionary mapping each destination key to its loaded data.
                When multiple destinations share the same key, only the first result is kept.

        Raises:
            IncompatibleStepsError: If the data keys do not match the destination keys.
            ETLStepError: If a destination fails while loading data.
        """
        results: dict[str, T] = {}
        data_keys = set(data.keys())
        destination_keys = set(self._destinations.keys())
        if data_keys != destination_keys:
            raise IncompatibleStepsError("transform", data_keys, "load", destination_keys)
        for name, destinations in self._destinations.items():
            data_to_load = data[name]
            for destination in destinations:
                try:
                    result = _load(data_to_load, destination, self._logger)
                except Exception as e:
                    raise ETLStepError("load", name, destination) from e
                if name not in results:
                    results[name] = result
        return results
