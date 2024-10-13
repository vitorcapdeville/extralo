import pandas as pd

from extralo.transformer import Transformer


class MyTransformer(Transformer):
    def transform(self, data):
        data["policy_start_date"] = pd.to_datetime(data["policy_start_date"])
        data["days_since_start"] = (pd.Timestamp.now() - data["policy_start_date"]).dt.days
        return {"data": data}
