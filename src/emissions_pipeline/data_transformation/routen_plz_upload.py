from google.cloud import bigquery
import pandas as pd
from loguru import logger
from settings import PROJECT_ID, DATASET_ID, ROUTEN_PLZ, CREDENTIALS_PATH, BUCKET_NAME, FILE_NAME


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
        df = pd.read_excel(self.excel_file_path, dtype={'PLZ_von': str, 'PLZ_nach': str})
        required_columns = ['Land_von', 'PLZ_von', 'Land_nach', 'PLZ_nach']

        # Select only the required columns if they exist
        if not set(required_columns).issubset(df.columns):
            raise ValueError(f"Excel file must contain columns: {required_columns}")

        df = df[required_columns]

        df['PLZ_von'] = df['PLZ_von'].astype(str)
        df['PLZ_nach'] = df['PLZ_nach'].astype(str)

        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{ROUTEN_PLZ}"

        job_config = bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_APPEND")
        job = self.client.load_table_from_dataframe(
            df, destination_table, job_config=job_config)
        job.result()
        logger.success(f"Data has been loaded successfully into {ROUTEN_PLZ}")

    def loading_into_area_code_bq_table(self):
        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{ROUTEN_PLZ}"
        URL = f"gs://{BUCKET_NAME}/{FILE_NAME}"
        self.client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS_PATH)

        job_config = bigquery.LoadJobConfig(create_disposition="CREATE_NEVER", write_disposition="WRITE_TRUNCATE")
        column_names = ['Land_von', 'Plz_von', 'Land_nach', 'Plz_nach']

        try:
            df = pd.read_excel(URL, names=column_names)
            df = df.astype(str)
            job = self.client.load_table_from_dataframe(df, destination_table, job_config=job_config)
            job.result()
            logger.success(f"Data has been loaded successfully into {ROUTEN_PLZ}")
        except Exception as e:
            logger.error(f"Error writing to BigQuery table: {e}")