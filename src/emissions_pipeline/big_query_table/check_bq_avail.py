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

    def check_or_create_dataset(self):
        """
        The function `check_or_create_dataset` checks if a dataset exists and creates it if it does not.
        """
        try:
            self.client.get_dataset(DATASET_ID)
            logger.info(f"Dataset {DATASET_ID} already exists.")
            self.client.delete_dataset(DATASET_ID, delete_contents=True, not_found_ok=True)
            logger.info("Previous dataset has been deleted and a new dataset has been successfully created.")
        except NotFound:
            logger.info(f"Dataset {DATASET_ID} not found. Creating dataset...")
            dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
            dataset_ref.location = "EU"
            self.client.create_dataset(dataset_ref)
            logger.info(f"Dataset {DATASET_ID} created.")

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
        self.check_or_create_dataset()
        self.check_or_create_table()