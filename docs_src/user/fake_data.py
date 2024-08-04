import pandas as pd

data = pd.DataFrame(
    {
        "client": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "policy_start_date": ["2024-01-01", "2024-02-02", "2024-03-03", "2024-04-04", "2024-05-05"],
    }
)

data.to_csv("data.csv", index=False)
