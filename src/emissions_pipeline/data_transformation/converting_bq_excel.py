import hashlib

from google.cloud import bigquery
from loguru import logger
from pandas_gbq import read_gbq
import pandas_gbq
import pandas as pd
from datetime import datetime
from src.config.settings import PROJECT_ID, DATASET_ID, TABLE_VIEW, ERROR


class BQLoading:
    def __init__(self, name, output_path):
        self.file_path_output = output_path
        self.file_name = name
        
    def transform_bq_table_to_xlsx(self):
        """
        Transforms a BigQuery table into an XLSX file and saves it locally.

        Parameters:
            PROJECT_ID (str): Your GCP project ID.
            DATASET_ID (str): The BigQuery dataset ID.
            TABLE_NAME (str): The BigQuery table name.
            FILE_NAME (str): The base name for the XLSX file.
            local_directory (str): The directory where the file will be saved (default is current directory).
        """

        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_VIEW}"

        dt = datetime.now()
        dt_string = dt.strftime("%Y_%m_%d_%H_%M_%S")

        file_name = f"{self.file_name}_{dt_string}.xlsx"
        local_file_path = f"{self.file_path_output}/{file_name}"

        query = f"SELECT * FROM `{destination_table}`"
        logger.info("Starting to process BQ table to xlsx...")

        try:
            df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, dialect='standard')
            logger.info(f"Fetched {len(df)} rows from BigQuery table: {destination_table}")
            df = df.astype(str)
            df.to_excel(local_file_path, index=False)
            logger.info(f"XLSX file saved locally at: {local_file_path}")
            self._create_sheet2_for_errors(local_file_path)
            return local_file_path

        except Exception as e:
            logger.error(f"Error while processing BQ table to XLSX: {e}")
            raise
    
    def _create_sheet2_for_errors(self,local_file_path):
        try:
            destination_table_error = f"{PROJECT_ID}.{DATASET_ID}.{ERROR}"
            query = f"SELECT * FROM `{destination_table_error}`"
            df_error = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, dialect='standard')
            with pd.ExcelWriter(local_file_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
                df_error.to_excel(writer, sheet_name='Error', index=False)
        except Exception as e:
            logger.error(f"Second sheet could not be created and appended! Error: {e}")