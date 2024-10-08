import logging

from extralo import ETL, CSVSource, SQLDestination
from sqlalchemy import create_engine
from transformer import MyTransformer

logging.basicConfig(level=logging.INFO)

engine = create_engine("sqlite:///data.sqlite")

etl = ETL(
    sources={
        "data": CSVSource(file="data.csv"),
    },
    transformer=MyTransformer(),
    destinations={
        "data": [
            SQLDestination(engine=engine, table="data_group", schema=None, if_exists="replace"),
        ],
    },
)

etl.execute()
