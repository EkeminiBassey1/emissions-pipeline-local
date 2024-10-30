import importlib.resources
import os

import pandas as pd
import pandas_gbq
import inflect
from google.cloud import bigquery
from src.emissions_pipeline.data_loading.run_sql_query import RunQueries
from loguru import logger
from pandas_gbq import read_gbq

import src.util.multiple_excel_files as multiple_excel_files
from settings import (CREDENTIALS_PATH, DATASET_ID, ERROR, FOLDER_NAME, PROJECT_ID, TABLE_VIEW, EXCEL_FILE_NAME)

class BQTransformation:
    def __init__(self, output_path):
        self.run_queries = RunQueries()
        self.file_path_output = output_path
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)
        self.local_file_path = f"{self.file_path_output}/{EXCEL_FILE_NAME}"
        self.p = inflect.engine()
        self.excel_max_row = 999999
        #ToDo. Fix the table name. So that views of DR and WR can be exported. 
        
    def transform_bq_table_to_xlsx(self, table:str):
        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{table}"
        query = f"SELECT * FROM `{destination_table}`"

        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, dialect='standard')
        df = df.astype(str)
        df_length = len(df)
        
        logger.info(f"Fetched {df_length} rows from BigQuery table: {destination_table}")
        logger.info("Starting to process BQ table to xlsx...")

        try:
            if df_length <= self.excel_max_row :
                logger.info("The table has less than 1 Million rows and can be stored in one file!")
                df.to_excel(self.local_file_path, engine='openpyxl', index=False)
                self._create_sheet2_for_errors(self.local_file_path)
            elif df_length > self.excel_max_row :
                self._more_than_million_rows(length=df_length, path=self.local_file_path, table_view=table)
        except Exception as e:
            logger.error(f"Error while processing BQ table to XLSX: {e}")
            raise

    def _create_sheet2_for_errors(self, local_file_path):
        try:
            destination_table_error = f"{PROJECT_ID}.{DATASET_ID}.{ERROR}"
            query = f"SELECT * FROM `{destination_table_error}`"
            df_error = pandas_gbq.read_gbq(
                query, project_id=PROJECT_ID, dialect='standard')
            with pd.ExcelWriter(local_file_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
                df_error.to_excel(writer, sheet_name='Error', index=False)
        except Exception as e:
            logger.error(
                f"Second sheet could not be created and appended! Error: {e}")

    def _create_multiple_queries_excel_files(self, parts_length: int, dataframe_length:int, table_view:str):
        list_parts_query = []
        offset = 0
        query = importlib.resources.read_text(multiple_excel_files, "create_excel_output_view.sql")
        logger.info("Creating the view for the excel files...")
        
        part_size = self._calculating_number_of_excel_files_excel_size(dataframe_length=dataframe_length, return_value="parts")

        for i in range(1, parts_length+1):
            file_name = f"{EXCEL_FILE_NAME}_{self._number_to_words_with_underscores(i)}"
            query_template = importlib.resources.read_text(multiple_excel_files, "create_excel_output_view.sql")
            query = query_template.replace("{$project_id}", PROJECT_ID) \
                .replace("{$dataset_id}", DATASET_ID) \
                .replace("{$excel_file_view_part}", file_name) \
                .replace("{$table_view}", table_view) \
                .replace("{$parts}", str(part_size)) \
                .replace("{$offset}", str(offset))
            offset = offset + part_size
            list_parts_query.append(query)
        else:
            logger.info("The table has less than 1 Million rows and can be stored in one file!") #ToDo
        return list_parts_query

    def _temp_table_to_excel(self, parts_length: int):
        query_template = importlib.resources.read_text(multiple_excel_files, "excel_file_view_parts.sql")
        self._create_new_folder_for_multiple_excel_files(file_path=self.file_path_output, name=FOLDER_NAME)

        logger.info("Exporting the excel views to excel file on-prem...")
        for i in range(1, parts_length + 1):
            file_name = f"{EXCEL_FILE_NAME}_{self._number_to_words_with_underscores(i)}"            
            query = query_template.replace("{$project_id}", PROJECT_ID).replace(
                "{$dataset_id}", DATASET_ID).replace("{$excel_file_view_part}", file_name)
            
            df = pandas_gbq.read_gbq(
                query, project_id=PROJECT_ID, dialect='standard')
            local_file_path = f"{self.file_path_output}/{FOLDER_NAME}/{EXCEL_FILE_NAME}_{i}.xlsx"
            df.to_excel(local_file_path, index=False)

    def _create_new_folder_for_multiple_excel_files(self, file_path: str, name: str):
        folder_name = f"{file_path}{name}"

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        folder_created = os.path.exists(folder_name)
        folder_created

    def _calculating_number_of_excel_files_excel_size(self, dataframe_length, return_value="counter"):
        counter = 1
        parts = dataframe_length
        if dataframe_length >= self.excel_max_row :
            while parts > self.excel_max_row :
                counter += 1
                parts = int(dataframe_length / counter)

        if return_value == "counter":
            return counter
        elif return_value == "parts":
            return parts
        else:
            raise ValueError(
                "Invalid return_value specified. Use 'counter' or 'parts'.")

    def _more_than_million_rows(self, length: int, path: str, table_view:str):
        logger.info("The table has more than 1 Million rows and can't be stored in one file.\n" +
                    "Therefore it will be splitted to multiple files.\n" +
                    "The process begins now...")
        excel_file_parts = self._calculating_number_of_excel_files_excel_size(length)
        list_queries = self._create_multiple_queries_excel_files(excel_file_parts, dataframe_length=length, table_view=table_view)
        self.run_queries.run_list_queries(queries=list_queries)
        self._temp_table_to_excel(excel_file_parts)
        logger.info(f"XLSX file saved locally at: {path}")

    def _number_to_words_with_underscores(self, number):
        words = self.p.number_to_words(number)
        return words.replace(" ", "_")