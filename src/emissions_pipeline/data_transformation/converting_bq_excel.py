from loguru import logger
from pandas_gbq import read_gbq
from google.cloud import bigquery

import os
import pandas_gbq
import pandas as pd
from datetime import datetime
import importlib.resources
import src.util.multiple_excel_files as multiple_excel_files
from settings import PROJECT_ID, DATASET_ID, TABLE_VIEW, ERROR, FOLDER_NAME, CREDENTIALS_PATH


class BQLoading:
    def __init__(self, name, output_path):
        self.file_path_output = output_path
        self.file_name = name
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)
        
    def transform_bq_table_to_xlsx(self):
        dt = datetime.now()
        dt_string = dt.strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f"{self.file_name}_{dt_string}.xlsx"
        local_file_path = f"{self.file_path_output}/{file_name}"
        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_VIEW}"
        query = f"SELECT * FROM `{destination_table}`"
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, dialect='standard')
        df = df.astype(str)
        df_length = len(df)
        
        logger.info(f"Fetched {len(df)} rows from BigQuery table: {destination_table}")
        logger.info("Starting to process BQ table to xlsx...")

        try:
            if df_length <= 999999:
                logger.info("The table has less than 1 Million rows and can be stored in one file!")
                df.to_excel(local_file_path, index=False)
                self._create_sheet2_for_errors(local_file_path)
            elif df_length > 999999:
                logger.info("The table has more than 1 Million rows and can't be stored in one file.\n "+
                            "Therefore it will be splitted to multiple files.\n"+
                            "The process begins now...")
                self._temp_table_to_excel(df_length)
            logger.info(f"XLSX file saved locally at: {local_file_path}")
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
        
    def _create_multiple_excel_files(self, file_length:int):
        counter = 1
        offset = 0
        list_parts_query = []
        parts = file_length
        query = importlib.resources.read_text(multiple_excel_files, "create_excel_output_view.sql")

        if file_length >= 1000000:
            while parts > 999999: 
                counter +=1 
                parts = int(file_length / counter)
            for i in range(1, counter+1):
                query = query.replace("{$project_id}", PROJECT_ID).replace("{$dataset_id}", DATASET_ID).replace("{$i}", str(i)).replace("{$table_view}", TABLE_VIEW).replace("{$parts}", str(parts)).replace("{$offset}", str(offset))
                offset = offset + parts
                query_job = self.client.query(query)
                results = query_job.result()
                list_parts_query.append(query)
            return list_parts_query
        else:
            logger.info("The table has less than 1 Million rows and can be stored in one file!")
    
    def _temp_table_to_excel(self, dataframe_length:int): 
        query = importlib.resources.read_text(multiple_excel_files, "excel_file_view_parts.sql")
        self._create_multiple_excel_files(dataframe_length)
        parts_amount = int(len(self._create_multiple_excel_files(dataframe_length)))
        self._create_new_folder_for_multiple_excel_files(file_path=self.file_path_output, name=FOLDER_NAME)
        
        for i in range(1, parts_amount): 
            query = query.replace("{$project_id}", PROJECT_ID).replace("{$dataset_id}", DATASET_ID).replace("{$i}", str(i))
            df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, dialect='standard')
            local_file_path = f"{self.file_path_output}/{FOLDER_NAME}/file_number_{i}.xlsx"
            df.to_excel(local_file_path, index=False)
            
    def _create_new_folder_for_multiple_excel_files(self, file_path:str, name:str): 
        folder_name = f"{file_path}{name}"

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        folder_created = os.path.exists(folder_name)
        folder_created