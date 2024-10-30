import hashlib

from google.cloud import bigquery
from loguru import logger
from pandas_gbq import read_gbq
from settings import PROJECT_ID, DATASET_ID, BASE_COORS

class BaseCoors:
    def __init__(self):
        self.destination_table_base_coors = f'{PROJECT_ID}.{DATASET_ID}.{BASE_COORS}'
        self.client = bigquery.Client(project=PROJECT_ID)
    
    def loading_bq_table_base_coors(self, TABLE: str):
        """
        The function `loading_bq_table_base_coors` loads data from a BigQuery table, generates an ID for
        each row, and writes the data to another BigQuery table.
        
        :param TABLE: The `TABLE` parameter in the `loading_bq_table_base_coors` function is a string that
        represents the name of the BigQuery table from which data will be loaded
        :type TABLE: str
        """
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE}`"
        df_base_coors = read_gbq(query, project_id=PROJECT_ID, dialect="standard")
        df_base_coors['ID'] = df_base_coors.apply(lambda row: hashlib.md5(
            ''.join(map(str, row)).encode()).hexdigest(), axis=1)

        job_config = bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER", write_disposition="WRITE_TRUNCATE")
        job = self.client.load_table_from_dataframe(
            df_base_coors, self.destination_table_base_coors, job_config=job_config)
        job.result()

        logger.success(f'Data has been successfully loaded into {BASE_COORS}!')
