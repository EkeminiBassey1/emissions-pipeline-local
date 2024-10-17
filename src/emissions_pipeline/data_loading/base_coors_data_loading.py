import yaml
import hashlib

from google.cloud import bigquery
from loguru import logger
from pandas_gbq import read_gbq


class Base_Coors:
    def __init__(self):
        project_data = yaml.safe_load(open('config.yaml'))
        self.project_id = project_data['project']['project_id']
        self.dataset_id = project_data['project']['dataset_id']    
        self.base_coors = project_data['project']['table_base_coors']
        self.routen_plz = project_data['project']['table_routen_plz']

    def loading_bq_table_base_coors():
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