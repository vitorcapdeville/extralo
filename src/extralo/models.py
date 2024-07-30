import logging
from typing import Any, Dict, Optional, Tuple

import pandera as pa
from pandera.typing.common import DataFrameBase

from extralo.destinations.file import CSVAppendDestination
from extralo.typing import DataFrame
from pandera.api.dataframe.model import TDataFrameModel, _is_field, _CONFIG_OPTIONS


class TolerableDataFrameModel(pa.DataFrameModel):
    class Config:
        failure_id = None
        failure_destination = CSVAppendDestination("failure_cases.csv")

    @classmethod
    def validate(
        cls: type[TDataFrameModel],
        check_obj: DataFrame,
        head: Optional[int] = None,
        tail: Optional[int] = None,
        sample: Optional[int] = None,
        random_state: Optional[int] = None,
        lazy: bool = False,
        inplace: bool = False,
    ) -> DataFrameBase[TDataFrameModel]:
        try:
            check_obj = super().validate(check_obj, head, tail, sample, random_state, lazy, inplace)
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
                f"Loaded {len(failure_cases)} failure cases of {cls.Config.failure_id} to {cls.Config.failure_destination}"
            )
        return check_obj

    @classmethod
    def _extract_config_options_and_extras(
        cls,
        config: Any,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        config_options, extras = {}, {}
        for name, value in vars(config).items():
            if name in _CONFIG_OPTIONS + ["failure_destination", "failure_id"]:
                config_options[name] = value
            elif _is_field(name):
                extras[name] = value

        return config_options, extras
