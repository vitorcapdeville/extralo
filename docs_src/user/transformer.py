import pandas as pd


def my_transformer(data):
    data["policy_start_date"] = pd.to_datetime(data["policy_start_date"])
    data["days_since_start"] = (pd.Timestamp.now() - data["policy_start_date"]).dt.days
    return {"data": data}
