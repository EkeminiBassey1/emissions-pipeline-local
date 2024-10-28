import os
import importlib.resources
import src.util.sql as queries_dh
import src.util.view as queries_view
import src.util.alternative_queries as queries_alternative_queries
from google.cloud import bigquery
from loguru import logger
from settings import PROJECT_ID, DATASET_ID, CREDENTIALS_PATH, BASE_WR_KILOMETRIERT


class RunQueries:
    def __init__(self):
        folder_path_sql = 'src/util/sql'
        folder_path_view = 'src/util/view'

        self.file_names = [f for f in os.listdir(folder_path_sql) if f.endswith(
            '.sql') and os.path.isfile(os.path.join(folder_path_sql, f)) and f != '__init__.sql']
        self.file_names_view = [f for f in os.listdir(folder_path_view) if f.endswith(
            '.sql') and os.path.isfile(os.path.join(folder_path_view, f)) and f != '__init__.sql']

        self.client = bigquery.Client(
            credentials=CREDENTIALS_PATH, project=PROJECT_ID)

    def run_queries(self, use_replacements=False, query_type="main"):
        if query_type == "main":
            file_names = self.file_names
            query_source = queries_dh
        elif query_type == "view":
            file_names = self.file_names_view
            query_source = queries_view

        for name in file_names:
            query = importlib.resources.read_text(query_source, name)

            if use_replacements:
                query = query.replace("{$project_id}", PROJECT_ID).replace(
                    "{$dataset_id}", DATASET_ID)

            query_job = self.client.query(query)
            results = query_job.result()
            logger.success(
                f"Table {PROJECT_ID}.{DATASET_ID}.{name} has been created!")

    def create_directRoute_view(self):
        logger.info("Creating view for Direct Route...")
        query_template = importlib.resources.read_text(queries_alternative_queries, "direct_route_query.sql")
        query = query_template.replace("{$project_id}", PROJECT_ID).replace("{$dataset_id}", DATASET_ID).replace("{$base_coors_wr_kilometriert}", BASE_WR_KILOMETRIERT)
        query_job = self.client.query(query)
        results = query_job.result()
        logger.success("The view has been created!")
    
    def run_list_queries(self, queries:list):
        for name in queries:
            query_job = self.client.query(name)
            logger.info(f"{name} has been created...")
            results = query_job.result()