import yaml
import hashlib

from google.cloud import bigquery
from loguru import logger
from pandas_gbq import read_gbq
import pandas_gbq
import pandas as pd
from datetime import datetime
import logging


class BaseCoors:
    def __init__(self, name, output_path):
        project_data = yaml.safe_load(open('config.yaml'))
        self.project_id = project_data['project']['project_id']
        self.dataset_id = project_data['project']['dataset_id']    
        self.base_coors = project_data['project']['table_base_coors']
        self.routen_plz = project_data['project']['table_routen_plz']
        self.table_view_xlsx = project_data['project']['table_view']
        self.file_path_output = output_path
        self.file_name = name

    def loading_bq_table_base_coors(self):
        """
        columns_to_insert:
        PROJECT_ID: wgs-emission-data-dev
        DATASET_ID: emissions_testing
        TABLE_INPUT_ID: adr_vonnach_komplett & routen_plz. The coordiante table and area code
        TABLE_BASE_COORS: base_coors -> bring the input tables adr_vonnach_komplett and routen_plz together and giving it an ID
        ['ID', 'Land_von', 'Plz_von', 'Land_nach', 'Plz_nach'], ['ID', 'VONLON', 'VONLAT', 'NACHLON', 'NACHLAT'] creating
        base_coors table and give the input table an ID
        """

        client = bigquery.Client(project=self.project_id)

        query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.routen_plz}`"
        df_base_coors = read_gbq(query, project_id=self.project_id, dialect="standard")

        df_base_coors['ID'] = df_base_coors.apply(lambda row: hashlib.md5(''.join(map(str, row)).encode()).hexdigest(),
                                                axis=1)
        destination_table_base_coors = f'{self.project_id}.{self.dataset_id}.{self.base_coors}'

        job_config = bigquery.LoadJobConfig(create_disposition="CREATE_NEVER", write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df_base_coors, destination_table_base_coors,
                                            job_config=job_config)
        job.result()

        logger.success(f'Data loaded into {self.base_coors}')
        
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

        destination_table = f"{self.project_id}.{self.dataset_id}.{self.table_view_xlsx}"

        dt = datetime.now()
        dt_string = dt.strftime("%Y_%m_%d_%H_%M_%S")

        file_name = f"{self.file_name}_{dt_string}.xlsx"
        local_file_path = f"{self.file_path_output}/{file_name}"

        query = f"SELECT * FROM `{destination_table}`"

        logger.info("Starting to process BQ table to xlsx...")

        try:
            df = pandas_gbq.read_gbq(query, project_id=self.project_id, dialect='standard')
            logger.info(f"Fetched {len(df)} rows from BigQuery table: {destination_table}")
            df = df.astype(str)
            df.to_excel(local_file_path, index=False)
            logger.info(f"XLSX file saved locally at: {local_file_path}")
            return local_file_path

        except Exception as e:
            logger.error(f"Error while processing BQ table to XLSX: {e}")
            raise