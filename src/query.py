import pandas as pd
import os
from google.cloud import bigquery
from dotenv import load_dotenv, dotenv_values 

load_dotenv()


def fdic_top_ten_banks_query():
    print("\nStarting fdic_top_ten_banks_query\n")
    FDIC_PROJECT_ID = bigquery.Client(project=os.getenv("FDIC_PROJECT"))

    QUERY = """
    SELECT * FROM (
    SELECT
        institution_name, 
        state_name, 
        address, 
        total_assets as total_assets_USD, 
        RANK() OVER (PARTITION BY state_name ORDER BY total_assets DESC) AS rank, 
        RANK() OVER (ORDER BY total_assets DESC) as national_rank 
    FROM `bigquery-public-data.fdic_banks.institutions`
    ) 
    WHERE rank <= 10
    """

    df = FDIC_PROJECT_ID.query(QUERY).to_dataframe()

    output_path = f"data/fdic_top_banks.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print("Finished fdic_top_ten_banks_query data saved to: ", output_path)
    return output_path