import os
import importlib.resources
import src.util.sql as queries_dh
import src.util.view as queries_view
from google.cloud import bigquery
from loguru import logger
from src.config.settings import PROJECT_ID, DATASET_ID, CREDENTIALS_PATH


class RunQueries: 
    def __init__(self): 
        folder_path_sql = 'src/util/sql'
        folder_path_view = 'src/util/view'
        
        self.file_names = [f for f in os.listdir(folder_path_sql) if f.endswith('.sql') and os.path.isfile(os.path.join(folder_path_sql, f)) and f != '__init__.sql']
        self.file_names_view = [f for f in os.listdir(folder_path_view) if f.endswith('.sql') and os.path.isfile(os.path.join(folder_path_view, f)) and f != '__init__.sql']
        
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)

    def run_queries(self, use_replacements=False, query_type="main"):
        if query_type == "main":
            file_names = self.file_names
            query_source = queries_dh
        else:
            file_names = self.file_names_view
            query_source = queries_view

        for name in file_names:
            query = importlib.resources.read_text(query_source, name)

            if use_replacements:
                query = query.replace("{$project_id}",PROJECT_ID).replace("{$dataset_id}", DATASET_ID)

            query_job = self.client.query(query)
            results = query_job.result()
            logger.info(f"{name} has been created.")