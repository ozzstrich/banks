import os
import logging
from google.cloud import bigquery
from dotenv import load_dotenv 

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "squp.log")),
        logging.StreamHandler()
    ]
)

load_dotenv()


def fdic_top_ten_banks_query():
    logging.info("Starting fdic_top_ten_banks_query")
    
    try:
        FDIC_PROJECT_ID = os.getenv("FDIC_PROJECT")
        if not FDIC_PROJECT_ID:
            logging.error("FDIC_PROJECT environment variable not set")
            return None
            
        client = bigquery.Client(project=FDIC_PROJECT_ID)
        logging.info("Successfully connected to BigQuery project: %s", FDIC_PROJECT_ID)
        
        QUERY = """
        SELECT * FROM (
        SELECT
            fdic_id as salesforce_id,
            fdic_id,
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
        
        try:
            logging.info("Executing BigQuery query")
            df = client.query(QUERY).to_dataframe()
            logging.info("Query executed successfully, retrieved %d rows", len(df))
        except Exception as e:
            logging.error("Query execution failed: %s", str(e))
            return None
        
        try:
            output_path = "data/fdic_top_banks_15.parquet"
            output_path_csv = "data/fdic_top_banks_15.csv"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.head(15).to_parquet(output_path, index=False)
            df.head(15).to_csv(output_path_csv, index=False)
            logging.info("Data successfully saved to: %s", output_path)
        except Exception as e:
            logging.error("Failed to save data to file: %s", str(e))
            return None
            
        logging.info("fdic_top_ten_banks_query completed successfully")
        return output_path
        
    except Exception as e:
        logging.error("Connection to BigQuery failed: %s", str(e))
        return None
