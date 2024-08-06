import logging
from typing import Any, Literal, Optional

from extralo.destination import Destination

try:
    import pandera as pa
except ImportError as err:
    raise ImportError(
        "pandera is required to use TolerableDataFrameModel. Please install it via `pip install pandera`."
    ) from err
import pandera.errors
from pandera.api.dataframe.model import TDataFrameModel, _is_field
from pandera.api.dataframe.model_config import BaseConfig

from extralo.typing import DataFrame


class TolerableConfig(BaseConfig):  # noqa: D101
    tolerate: Optional[set[Literal["DATA", "SCHEMA"]]] = None
    failure_id: Optional[str] = None
    failure_destination: Optional[Destination] = None


_CONFIG_OPTIONS = [attr for attr in vars(TolerableConfig) if _is_field(attr)] + [
    attr for attr in vars(BaseConfig) if _is_field(attr)
]


class TolerableDataFrameModel(pa.DataFrameModel):  # noqa: D101
    Config: type[BaseConfig] = TolerableConfig

    @classmethod
    def validate(  # noqa: D102
        cls: type[TDataFrameModel],
        check_obj: DataFrame,
        head: Optional[int] = None,
        tail: Optional[int] = None,
        sample: Optional[int] = None,
        random_state: Optional[int] = None,
        lazy: bool = False,
        inplace: bool = False,
    ) -> DataFrame:
        try:
            return super().validate(check_obj, head, tail, sample, random_state, lazy, inplace)
        except pandera.errors.SchemaErrors as errors:
            if cls.Config.tolerate is None:
                cls.Config.tolerate = set()
            not_tolerable_errors_found = set(errors.message.keys()) - set(cls.Config.tolerate)

            if len(not_tolerable_errors_found) > 0:
                logging.getLogger("etl").error(
                    f"Found these errors {not_tolerable_errors_found} but only configured "
                    f"to tolerate these errors {cls.Config.tolerate}"
                )
                raise errors from errors

            if cls.Config.failure_destination is None:
                return check_obj

            failure_cases = (
                errors.failure_cases[["index", "check"]].drop_duplicates().sort_values("index").set_index("index")
            )
            failure_cases.index.name = None
            failure_cases = errors.data.merge(
                failure_cases, left_index=True, right_index=True, how="right"
            ).reset_index(drop=True)
            if cls.Config.failure_id is None:
                cls.Config.failure_id = cls.__name__
            failure_cases["name"] = cls.Config.failure_id
            cls.Config.failure_destination.load(failure_cases)
            logging.getLogger("etl").info(
                f"Loaded {len(failure_cases)} failure cases of {cls.Config.failure_id} to "
                f"{cls.Config.failure_destination}"
            )
        return check_obj

    @classmethod
    def _extract_config_options_and_extras(
        cls,
        config: Any,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        config_options, extras = {}, {}
        for name, value in vars(config).items():
            if name in _CONFIG_OPTIONS:
                config_options[name] = value
            elif _is_field(name):
                extras[name] = value

        return config_options, extras
