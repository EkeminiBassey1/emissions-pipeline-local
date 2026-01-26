import importlib.resources
import os

import pandas as pd
import pandas_gbq
import inflect
from google.cloud import bigquery
from src.emissions_pipeline.data_loading.run_sql_query import RunQueries
from loguru import logger
from pandas_gbq import read_gbq

import src.util.sql_queries.multiple_excel_files as multiple_excel_files
from settings import (CREDENTIALS_PATH, DATASET_ID, ERROR, FOLDER_NAME, PROJECT_ID, EXCEL_FILE_NAME)

class BQTransformation:
    def __init__(self, output_path):
        self.run_queries = RunQueries()
        self.file_path_output = output_path
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)
        self.local_file_path = f"{self.file_path_output}/{EXCEL_FILE_NAME}"
        self.p = inflect.engine()
        self.excel_max_row = 999999
                
    def transform_bq_table_to_xlsx(self, table:str):
        """
        The function `transform_bq_table_to_xlsx` reads data from a BigQuery table, processes it, and saves
        it to an Excel file, handling cases where the data exceeds a certain row limit.
        
        :param table: The `table` parameter in the `transform_bq_table_to_xlsx` function is a string that
        represents the name of the BigQuery table that you want to transform into an Excel file
        :type table: str
        """
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
        """
        The function `_create_sheet2_for_errors` reads data from a BigQuery table and appends it to an Excel
        sheet named 'Error'.
        
        :param local_file_path: The `local_file_path` parameter in the `_create_sheet2_for_errors` method is
        the file path where you want to save the Excel file containing the error data. This parameter should
        be a string representing the local file path on your system where the Excel file will be created or
        updated
        """
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
        """
        The function `_create_multiple_queries_excel_files` generates multiple SQL queries for creating
        views in Excel files based on specified parameters.
        
        :param parts_length: The `parts_length` parameter in the `_create_multiple_queries_excel_files`
        method represents the total number of parts or sections into which the data will be divided for
        processing and creating multiple Excel files
        :type parts_length: int
        :param dataframe_length: The `dataframe_length` parameter in the
        `_create_multiple_queries_excel_files` method represents the length of the dataframe for which you
        want to create multiple queries for Excel files. This parameter is used to calculate the number of
        parts or Excel files that will be created based on the size of the dataframe
        :type dataframe_length: int
        :param table_view: The `table_view` parameter in the `_create_multiple_queries_excel_files` method
        is a string that represents the name of the table or view in your database from which you want to
        create multiple Excel files. This parameter is used in the SQL query template to specify the source
        table or view for each Excel
        :type table_view: str
        :return: The function `_create_multiple_queries_excel_files` returns a list of queries that are used
        to create views for multiple Excel files based on the specified parts length, dataframe length, and
        table view.
        """
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
        return list_parts_query

    def _temp_table_to_excel(self, parts_length: int):
        """
        This function exports multiple Excel views to Excel files on-premises based on a given parts length.
        
        :param parts_length: The `parts_length` parameter in the `_temp_table_to_excel` method represents
        the number of parts or sections that the data will be divided into when exporting to Excel files.
        This parameter is used in a loop to generate multiple Excel files based on the specified length
        :type parts_length: int
        """
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
        """
        This function creates a new folder for multiple Excel files if it does not already exist.
        
        :param file_path: The `file_path` parameter is a string that represents the path where the new
        folder will be created. It should be the directory path where you want to create the new folder
        :type file_path: str
        :param name: The `name` parameter in the `_create_new_folder_for_multiple_excel_files` function is
        used to specify the name of the new folder that will be created
        :type name: str
        """
        folder_name = f"{file_path}{name}"

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        folder_created = os.path.exists(folder_name)
        folder_created

    def _calculating_number_of_excel_files_excel_size(self, dataframe_length, return_value="counter"):
        """
        The function calculates the number of Excel files needed to store a given amount of data based on
        the maximum number of rows per Excel file.
        
        :param dataframe_length: The `dataframe_length` parameter in the
        `_calculating_number_of_excel_files_excel_size` function represents the total number of rows in a
        DataFrame that you want to split into multiple Excel files based on a maximum row limit
        (`self.excel_max_row`). The function calculates the number of Excel files needed
        :param return_value: The `return_value` parameter in the
        `_calculating_number_of_excel_files_excel_size` method specifies what value should be returned from
        the method. It can have two valid values:, defaults to counter (optional)
        :return: either the `counter` value or the `parts` value based on the `return_value` parameter
        provided to the function.
        """
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
        """
        This function splits a table with more than 1 million rows into multiple Excel files and saves them
        locally.
        
        :param length: The `length` parameter in the `_more_than_million_rows` method likely represents the
        total number of rows in the table that is being processed. It is used to determine if the table has
        more than 1 million rows and needs to be split into multiple files for storage or processing
        :type length: int
        :param path: The `path` parameter in the `_more_than_million_rows` method is a string that
        represents the local path where the XLSX file will be saved
        :type path: str
        :param table_view: The `table_view` parameter in the `_more_than_million_rows` method is used to
        specify the name or identifier of the table or view that contains the data to be processed. It is
        likely used within the method to interact with the specific table or view within the database or
        data source being accessed
        :type table_view: str
        """
        logger.info("The table has more than 1 Million rows and can't be stored in one file.\n" +
                    "Therefore it will be splitted to multiple files.\n" +
                    "The process begins now...")
        excel_file_parts = self._calculating_number_of_excel_files_excel_size(length)
        list_queries = self._create_multiple_queries_excel_files(excel_file_parts, dataframe_length=length, table_view=table_view)
        self.run_queries.run_list_queries(queries=list_queries)
        self._temp_table_to_excel(excel_file_parts)
        logger.info(f"XLSX file saved locally at: {path}")

    def _number_to_words_with_underscores(self, number):
        """
        The function converts a number to words and replaces spaces with underscores.
        
        :param number: The function `_number_to_words_with_underscores` takes a number as input and converts
        it to words using the `number_to_words` method from the `self.p` object. It then replaces spaces in
        the resulting words with underscores before returning the modified string
        :return: The function `_number_to_words_with_underscores` takes a number as input, converts it to
        words using the `number_to_words` method from the `self.p` object, and then replaces spaces with
        underscores in the resulting words. The function returns the words with underscores instead of
        spaces.
        """
        words = self.p.number_to_words(number)
        return words.replace(" ", "_")