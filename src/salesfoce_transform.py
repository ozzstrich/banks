import pandas as pd
from pathlib import Path


def sf_transform_fdic_banks():
    print("Starting sf_transform_fdic_banks")
    data_path = Path(__file__).resolve().parent.parent / "data" / "fdic_top_banks.parquet"
    df = pd.read_parquet(data_path)
    print(df.head(10))
    print("Finished sf_transform_fdic_banks")
