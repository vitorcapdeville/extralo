from models import after_schema, before_schema
from sqlalchemy import create_engine
from transformer import MyTransformer

from extralo import ETL, CSVSource, SQLDestination

engine = create_engine("sqlite:///data.sqlite")

etl = ETL(
    sources={"data": CSVSource("data.csv")},
    before_schemas={"data": before_schema},
    transformer=MyTransformer(),
    after_schemas={"data": after_schema},
    destinations={
        "data": [
            SQLDestination(engine, "data_group", None, if_exists="replace"),
        ],
    },
)

etl.execute()
