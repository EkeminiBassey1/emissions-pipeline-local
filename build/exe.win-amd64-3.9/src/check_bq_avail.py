from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from loguru import logger
from src.emissions_pipeline.big_query_table.create_bq_wr_table import BigQuery
from settings import PROJECT_ID, DATASET_ID, BASE_WR_KILOMETRIERT, CREDENTIALS_PATH


# The `BQOperations` class initializes BigQuery operations with specified project and dataset
# references.
class BQOperations:
    def __init__(self):
        self.bq_creator = BigQuery()
        self.dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)
        self.location = "EU"
    
    def ensure_dataset_recreated(self, dataset_name):
        try:
            dataset = self.client.get_dataset(dataset_name)
            logger.info(f"Dataset {dataset_name} exists.")

            while True:
                delete_confirmation = input(f"Dataset {dataset_name} exists. Do you want to delete it? (Y/N): ").strip().upper()
                if delete_confirmation == "Y":
                    logger.info(f"User confirmed to delete the dataset {dataset_name}.")
                    self.client.delete_dataset(dataset_name, delete_contents=True)
                    logger.info(f"Dataset {dataset_name} deleted successfully.")
                    break 
                elif delete_confirmation == "N":
                    logger.info(f"User chose not to delete the dataset {dataset_name}. Skipping deletion and dataset creation.")
                    return
                else:
                    logger.warning("Invalid input. Please enter 'Y' for Yes or 'N' for No.")

        except NotFound:
            logger.info(f"Dataset {dataset_name} does not exist. Proceeding to create a new dataset...")

        dataset_ref = self.client.dataset(dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset)
        logger.info(f"Dataset {dataset_name} created successfully in location {self.location}.")
        
    def check_or_create_table(self):
        """
        The function checks if a table exists in BigQuery and creates it if it does not.
        """
        table_ref = f"{DATASET_ID}.{BASE_WR_KILOMETRIERT}"
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists!")
        except NotFound:
            logger.info(f"Table {table_ref} not found. Creating table...")
            self.bq_creator.create_bigquery_table()

    def check_dataset_table(self):
        """
        The function `check_dataset_table` checks if a dataset exists and creates it if not, then checks if
        a table exists and creates it if not.
        """
        self.ensure_dataset_recreated(dataset_name=DATASET_ID)
        self.check_or_create_table()