import os
import yaml
import importlib.resources
import src.util.sql as queries_dh
import src.util.view as queries_view
from google.cloud import bigquery
from google.oauth2 import service_account
from loguru import logger


class RunQueries: 
    def __init__(self, key_file): 
        folder_path_sql = 'src/util/sql'
        folder_path_view = 'src/util/view'
        
        self.file_names = [f for f in os.listdir(folder_path_sql) if f.endswith('.sql') and os.path.isfile(os.path.join(folder_path_sql, f)) and f != '__init__.sql']
        self.file_names_view = [f for f in os.listdir(folder_path_view) if f.endswith('.sql') and os.path.isfile(os.path.join(folder_path_view, f)) and f != '__init__.sql']

        project_data = yaml.safe_load(open('config.yaml'))
        credentials = service_account.Credentials.from_service_account_file(
            key_file, 
            scopes=["https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/pubsub", 
                    "https://www.googleapis.com/auth/cloud-platform"]
        )
        self.client = bigquery.Client(credentials=credentials, project=project_data['project']['project_id'])

    def run_queries_on_bg(self):
        for name in self.file_names:
            query = importlib.resources.read_text(queries_dh, name)  
            
            logger.info(query)  
            query_job = self.client.query(query)

            results = query_job.result()
            logger.info(f"{name} has been created.")
    
    def run_queries_view(self): 
        for name in self.file_names_view:
            
            print(self.file_names_view)
            query = importlib.resources.read_text(queries_view, name)  
            
            logger.info(query)  
            query_job = self.client.query(query)

            results = query_job.result()
            logger.info(f"{name} has been created.")