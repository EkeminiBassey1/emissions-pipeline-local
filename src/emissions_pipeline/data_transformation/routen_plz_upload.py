import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from google import auth
from loguru import logger

key_path = "C:/Users/bassey/Downloads/wgs-emission-data-dev.json"
project_id = "wgs-emission-data-dev"
dataset_id = "emissions_testing"
table_routen_plz = "routen_plz"
destination_table = f"{project_id}.{dataset_id}.{table_routen_plz}"

class RoutenPLZ:
    def __init__(self, key_file, excel_file_path):
        self.excel_file_path = excel_file_path
        project_data = yaml.safe_load(open('config.yaml'))
        self.project_id = project_data['project']['project_id']
        self.dataset_id = project_data['project']['dataset_id']    
        self.base_coors = project_data['project']['table_base_coors']
        self.routen_plz = project_data['project']['table_routen_plz']
        credentials = service_account.Credentials.from_service_account_file(
            key_file, 
            scopes=["https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/pubsub", 
                    "https://www.googleapis.com/auth/cloud-platform"]
        )
        self.client = bigquery.Client(credentials=credentials, project=project_data['project']['project_id'])

    def routen_plz_excle_file_upload(self):
        df = pd.read_excel(self.excel_file_path)
        df['PLZ_von'] = df['PLZ_von'].astype(str)
        df['PLZ_nach'] = df['PLZ_nach'].astype(str)
        
        destination_table = f"{self.project_id}.{self.dataset_id}.{self.routen_plz}"
        
        job_config = bigquery.LoadJobConfig(create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_APPEND")
        job = self.client.load_table_from_dataframe(df, destination_table, job_config=job_config)
        job.result()
        logger.success(f"Data has been loaded successfully into {self.routen_plz}")