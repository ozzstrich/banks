import os
import logging
from simple_salesforce import Salesforce
from dotenv import load_dotenv

load_dotenv()


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

creds = {
        "username":os.getenv("SALESFORCE_USERNAME"),
        "password":os.getenv("SALESFORCE_PASSWORD"),
        "security_token":os.getenv("SALESFORCE_SECURITY_TOKEN"),
        "domain":os.getenv("SALESFORCE_DOMAIN")
        }

def sf_upload(dict_data):
    print("Starting sf_upload")
    try:
        sf = Salesforce(
            username=creds["username"],
            password=creds["password"],
            security_token=creds["security_token"],
            domain=creds["domain"]
        )
        logging.info("Connected to Salesforce")
    except Exception as e:
        logging.info(f"Error connecting to Salesforce: {e}")
        return None

    return(sf)