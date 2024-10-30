import os
import importlib.resources
import src.util.sql_queries.main_queries as queries_dh
import src.util.sql_queries.view as queries_view
import src.util.sql_queries.direct_route_query as queries_alternative_queries
from google.cloud import bigquery
from loguru import logger
from settings import PROJECT_ID, DATASET_ID, CREDENTIALS_PATH, BASE_WR_KILOMETRIERT


class RunQueries:
    def __init__(self):
        folder_path_sql = 'src/util/sql_queries/main_queries'
        folder_path_view = 'src/util/sql_queries/view'
        
        self.file_names = [
            f for f in os.listdir(folder_path_sql)
            if f.endswith('.sql') and os.path.isfile(os.path.join(folder_path_sql, f)) and f != '__init__.sql'
        ]
        self.file_names_view = [
            f for f in os.listdir(folder_path_view)
            if f.endswith('.sql') and os.path.isfile(os.path.join(folder_path_view, f)) and f != '__init__.sql'
        ]

        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)

    def run_queries(self, use_replacements=False, query_type="main"):
        """
        The `run_queries` function executes SQL queries using specified query sources and performs optional
        string replacements before creating tables and logging success messages.
        
        :param use_replacements: The `use_replacements` parameter in the `run_queries` method is a boolean
        flag that determines whether placeholders in the SQL queries should be replaced with actual values
        before executing the queries. If `use_replacements` is set to `True`, the method will replace
        placeholders like `{}`, defaults to False (optional)
        :param query_type: The `query_type` parameter in the `run_queries` method specifies whether to use
        queries from the "main" or "view" category. If `query_type` is set to "main", the method will use
        `file_names` and `queries_dh` for queries. If `query, defaults to main (optional)
        """
        if query_type == "main":
            file_names = self.file_names
            query_source = queries_dh
        elif query_type == "view":
            file_names = self.file_names_view
            query_source = queries_view
        else:
            raise ValueError("Invalid query_type specified. Choose 'main' or 'view'.")

        for name in file_names:
            query = importlib.resources.read_text(query_source, name)

            if use_replacements:
                query = query.replace("{$project_id}", PROJECT_ID).replace("{$dataset_id}", DATASET_ID)

            query_job = self.client.query(query)
            results = query_job.result()
            logger.success(f"Table {PROJECT_ID}.{DATASET_ID}.{name} has been created!")

    def create_directRoute_view(self):
# The code snippet you provided is a method called `create_directRoute_view` within the `RunQueries`
# class. Here's what it does:
        logger.info("Creating view for Direct Route...")
        query_template = importlib.resources.read_text(queries_alternative_queries, "direct_route_query.sql")
        query = query_template.replace("{$project_id}", PROJECT_ID).replace("{$dataset_id}", DATASET_ID).replace("{$base_coors_wr_kilometriert}", BASE_WR_KILOMETRIERT)
        query_job = self.client.query(query)
        results = query_job.result()
        logger.success("The view has been created!")
    
    def run_list_queries(self, queries:list):
        """
        The function `run_list_queries` iterates through a list of query names, runs each query using a
        client, logs the creation of each query, and retrieves the results.
        
        :param queries: The `queries` parameter in the `run_list_queries` method is expected to be a list of
        query names. The method iterates over each query name in the list, runs the query using the
        `self.client.query()` method, logs a message indicating that the query has been created, and then
        :type queries: list
        """
        for name in queries:
            query_job = self.client.query(name)
            logger.info(f"{name} has been created...")
            results = query_job.result()