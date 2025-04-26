import pandera as pa


class BeforeSchema(pa.DataFrameModel):
    client: str
    policy_start_date: str


class AfterSchema(pa.DataFrameModel):
    client: str
    policy_start_date: pa.DateTime
    days_since_start: int
