from google.cloud import bigquery
import pandas as pd
from loguru import logger
from settings import PROJECT_ID, DATASET_ID, ROUTEN_PLZ, CREDENTIALS_PATH


class RoutenPLZ:
    def __init__(self, excel_file_path):
        self.excel_file_path = excel_file_path
        self.client = bigquery.Client(
            credentials=CREDENTIALS_PATH, project=PROJECT_ID)

    def routen_plz_excle_file_upload(self):
        """
        This Python function reads an Excel file, converts specific columns to strings, and uploads the data
        to a BigQuery table.
        """
        df = pd.read_excel(self.excel_file_path)
        df['PLZ_von'] = df['PLZ_von'].astype(str)
        df['PLZ_nach'] = df['PLZ_nach'].astype(str)

        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{ROUTEN_PLZ}"

        job_config = bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_APPEND")
        job = self.client.load_table_from_dataframe(
            df, destination_table, job_config=job_config)
        job.result()
        logger.success(f"Data has been loaded successfully into {ROUTEN_PLZ}")