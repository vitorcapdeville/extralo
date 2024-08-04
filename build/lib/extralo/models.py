import logging
from typing import Any, Optional

try:
    import pandera as pa
except ImportError as err:
    raise ImportError(
        "pandera is required to use TolerableDataFrameModel. Please install it via pip install pandera."
    ) from err
from pandera.api.dataframe.model import _CONFIG_OPTIONS, TDataFrameModel, _is_field

from extralo.destinations.file import CSVAppendDestination
from extralo.typing import DataFrame


class TolerableDataFrameModel(pa.DataFrameModel):  # noqa: D101
    class Config:  # noqa: D106
        failure_id: Optional[str] = None
        failure_destination = CSVAppendDestination("failure_cases.csv")

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
        except pa.errors.SchemaErrors as errors:
            errors.message.pop("DATA", None)
            if errors.message != {}:
                raise errors from errors
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
            if name in _CONFIG_OPTIONS + ["failure_destination", "failure_id"]:
                config_options[name] = value
            elif _is_field(name):
                extras[name] = value

        return config_options, extras
