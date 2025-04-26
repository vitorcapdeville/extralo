from loguru import logger
from sqlalchemy import create_engine
from transformer import my_transformer

from extralo import ETL, CSVSource, SQLDestination

logger.enable("extralo")

engine = create_engine("sqlite:///data.sqlite")

etl = ETL(
    sources={
        "data": CSVSource(file="data.csv"),
    },
    transformer=my_transformer,
    destinations={
        "data": [
            SQLDestination(engine=engine, table="data_group", schema=None, if_exists="replace"),
        ],
    },
    name="basic_etl_with_logging",
)

etl.execute()
