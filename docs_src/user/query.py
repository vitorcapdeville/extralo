import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///data.sqlite")

print(pd.read_sql("data_group", engine))
