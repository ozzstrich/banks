import pandas as pd
import logging
import os
from pathlib import Path
from itertools import islice


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


def sf_transform_fdic_banks():
    logging.info("Starting sf_transform_fdic_banks")
    data_path = Path(__file__).resolve().parent.parent / "data" / "fdic_top_banks.parquet"
    try:
        df = pd.read_parquet(data_path)
        dict_data = df.to_dict()
    except Exception as e:
        logging.error("Error reading parquet file: %s", str(e))
        return None
    print(df.head(10), "\n")
    logging.info("Finished sf_transform_fdic_banks")
    return(dict_data)
