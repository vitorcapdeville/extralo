import pandera as pa

before_schema = pa.DataFrameSchema(
    {
        "client": pa.Column(pa.String),
        "policy_start_date": pa.Column(pa.DateTime),
    }
)

after_schema = pa.DataFrameSchema(
    {
        "client": pa.Column(pa.String),
        "policy_start_date": pa.Column(pa.DateTime),
        "days_since_start": pa.Column(pa.Int),
    }
)
