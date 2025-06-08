import pandas as pd
import os
from google.cloud import bigquery
from dotenv import load_dotenv, dotenv_values 

load_dotenv()

project_id = os.getenv("FDIC_PROJECT")
client = bigquery.Client(project=project_id)

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

df = client.query(QUERY).to_dataframe()
print(df.head())

