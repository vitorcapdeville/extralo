import inspect
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, _TypedDictMeta, get_type_hints

import pandera as pa

from extralo.destination import Destination
from extralo.source import Source
from extralo.transformer import Transformer
from extralo.transformers import NullTransformer
from extralo.typing import DataFrame


class IncompatibleStepsError(Exception):
    def __init__(self, step_base: str, keys_base: set, step: str, keys: set) -> None:
        self.step_base = step_base
        self.step = step
        self.keys_base = keys_base
        self.keys = keys

    def __str__(self) -> str:
        return f"Step '{self.step}' with keys {self.keys} is incompatible with step '{self.step_base}' with keys {self.keys_base}"


def validate_steps(step1_keys: set, step1_name: str, step2_keys: set, step2_name: str):
    if step1_keys != step2_keys:
        raise IncompatibleStepsError(step1_name, step1_keys, step2_name, step2_keys)


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

        trasnform_args = inspect.getargs(self._transformer.transform.__code__).args
        if self._before_schemas is not None:
            validate_steps(set(self._sources.keys()), "extract", set(self._before_schemas.keys()), "before_schema")
        validate_steps(set(self._sources.keys()), "extract", set(trasnform_args[1:]), "transform")

        transform_output = get_type_hints(self._transformer.transform).get("return", None)
        if transform_output is None or not isinstance(transform_output, _TypedDictMeta):
            logging.getLogger("etl").warning(
                "Transformer output type hints are not a TypedDict, validation will be done only at runtime."
            )
            return

        transform_output_dict = transform_output.__annotations__
        if self._after_schemas is not None:
            validate_steps(
                set(transform_output_dict.keys()), "transform", set(self._after_schemas.keys()), "after_schema"
            )
        validate_steps(set(transform_output_dict.keys()), "transform", set(self._destinations.keys()), "load")

    def execute(self) -> DataFrame:
        data = self.extract()
        data = self.before_validate(data)
        data = self.transform(data)
        data = self.after_validate(data)
        self.load(data)

    def extract(self) -> dict[str, DataFrame]:
        data = {}
        futures = []
        logger = logging.getLogger("etl")

        with ThreadPoolExecutor(max_workers=5) as executor:
            for name, source in self._sources.items():
                logger.info(f"Starting extraction for {source}")
                future = executor.submit(source.extract)
                futures.append((name, future))

        for name, future in futures:
            data[name] = future.result()  # Espera a conclusão do trabalho
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

        validate_steps(set(data.keys()), "transform", set(self._after_schemas.keys()), "after_schema")

        return {name: schema.validate(data[name], lazy=True) for name, schema in self._after_schemas.items()}

    def load(self, data: dict[str, DataFrame]) -> None:
        logger = logging.getLogger("etl")

        validate_steps(set(data.keys()), "transform", set(self._destinations.keys()), "load")

        futures = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            for name, destinations in self._destinations.items():
                data_to_load = data[name]
                size = len(data_to_load)
                for destination in destinations:
                    logger.info(f"Starting load of {size} records to {destination}")
                    future = executor.submit(destination.load, data_to_load)
                    futures.append((size, destination, future))

        for size, dest, future in futures:
            future.result()  # Espera a conclusão do trabalho
            logger.info(f"Loaded {size} records to {dest}")
