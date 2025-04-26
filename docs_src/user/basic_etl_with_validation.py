import pandas as pd
from models import AfterSchema, BeforeSchema
from pandera.typing import DataFrame
from pydantic import validate_call
from sqlalchemy import create_engine

from extralo import ETL, CSVSource, SQLDestination

engine = create_engine("sqlite:///data.sqlite")


@validate_call(validate_return=True)
def my_transformer(data: DataFrame[BeforeSchema]) -> dict[str, DataFrame[AfterSchema]]:
    data["policy_start_date"] = pd.to_datetime(data["policy_start_date"])
    data["days_since_start"] = (pd.Timestamp.now() - data["policy_start_date"]).dt.days
    return {"data": data}


etl = ETL(
    sources={"data": CSVSource("data.csv")},
    transformer=my_transformer,
    destinations={
        "data": [
            SQLDestination(engine, "data_group", None, if_exists="replace"),
        ],
    },
)

etl.execute()
