from sqlalchemy import create_engine
from transformer import my_transformer

from extralo import ETL, CSVSource, SQLDestination

engine = create_engine("sqlite:///data.sqlite")

etl = ETL(
    sources={
        "data": CSVSource("data.csv"),
    },
    transformer=my_transformer,
    destinations={
        "data": [
            SQLDestination(engine, "data_group", None, if_exists="replace"),
        ],
    },
)

etl.execute()
