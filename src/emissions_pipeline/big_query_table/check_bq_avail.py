import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from loguru import logger
from src.emissions_pipeline.big_query_table.create_bq_wr_table import BigQuery
from src.config.settings import PROJECT_ID, DATASET_ID, BASE_COORS, BASE_WR_KILOMETRIERT, ERROR, URL_WR, URL_DR, BATCH_SIZE, CREDENTIALS_PATH


class BQOperations: 
    def __init__(self):
        self.bq_creator = BigQuery()
        self.dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)
    
    def check_or_create_dataset(self):
        """Check if a dataset exists, and create it if it does not."""
        try:
            self.client.get_dataset(DATASET_ID)
            logger.info(f"Dataset {DATASET_ID} already exists.")
        except NotFound:
            logger.info(f"Dataset {DATASET_ID} not found. Creating dataset...")
            dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
            dataset_ref.location = "EU"
            self.client.create_dataset(dataset_ref)
            logger.info(f"Dataset {DATASET_ID} created.")

    def check_or_create_table(self):
        """Check if a table exists, and create it if it does not."""
        table_ref = f"{DATASET_ID}.{BASE_WR_KILOMETRIERT}"        
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists.")
        except NotFound:
            logger.info(f"Table {table_ref} not found. Creating table.")
            self.bq_creator.create_bigquery_table()
            
    def check_dataset_table(self):
        self.check_or_create_dataset()
        self.check_or_create_table()