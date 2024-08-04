import json
import logging.config

from extralo import ETL, CSVSource, SQLDestination
from sqlalchemy import create_engine
from transformer import MyTransformer

with open("logging.json") as f_in:
    config = json.load(f_in)

logging.config.dictConfig(config)

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
