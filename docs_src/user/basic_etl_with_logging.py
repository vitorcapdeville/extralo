import logging

from sqlalchemy import create_engine
from transformer import MyTransformer

from extralo import ETL, CSVSource, SQLDestination

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
