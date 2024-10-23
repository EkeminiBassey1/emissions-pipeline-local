from pandas_gbq import read_gbq
from loguru import logger
from google.cloud import bigquery

from src.emissions_pipeline.data_transformation.base_coors_uploading import loading_bq_table_base_coors
from src.emissions_pipeline.data_loading.data_bigquery_upload import BigQUpload
from src.config.settings import PROJECT_ID, DATASET_ID, BASE_WR_KILOMETRIERT, ERROR, ERROR_RATE_TOL, BATCH_SIZE, CREDENTIALS_PATH

class ReRun: 
    def __init__(self):
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)

    def re_run_failed_requests(self, url:str):
        uploading_to_bq = BigQUpload()
        perccentage_error = self._check_length_error_table()
        new_batch_size = BATCH_SIZE
        if perccentage_error <= ERROR_RATE_TOL:
            logger.info(f"The current error rate is {perccentage_error}% for the {ERROR} table. A re-run will not be continued. Calculation has been concluded.")
        elif perccentage_error >= ERROR_RATE_TOL:
            logger.info(f"The current error rate  is at {perccentage_error}% for the {ERROR} table. A re-run of the calculation will be initiated now...")
            while perccentage_error >= ERROR_RATE_TOL:
                new_batch_size = int(new_batch_size / 2)
                loading_bq_table_base_coors(TABLE=ERROR)
                self._truncate_error_table()
                uploading_to_bq.update_base_area_code_kilometrierung_table(base_coors_wr_kilometriert=BASE_WR_KILOMETRIERT, batch_size=new_batch_size ,url=url)
                perccentage_error = self._check_length_error_table()
                logger.info(f"The error rate is now: {perccentage_error}!")
                if perccentage_error <= ERROR_RATE_TOL:
                    logger.info(f"The current error rate is {perccentage_error}% for the {ERROR} table. A re-run will not be continued. Calculation has been concluded.")
                    break

    def _check_length_error_table(self):
        query_base_wr = f"SELECT count(ID) FROM {PROJECT_ID}.{DATASET_ID}.{BASE_WR_KILOMETRIERT}"
        query_error = f"SELECT count(ID) FROM {PROJECT_ID}.{DATASET_ID}.{ERROR}"
        
        df_result = read_gbq(query_base_wr, project_id=PROJECT_ID, dialect="standard")
        df_error = read_gbq(query_error, project_id=PROJECT_ID, dialect="standard")
        
        result_variable = df_result['f0_'].iloc[0]
        result_variable_error = df_error['f0_'].iloc[0] 
        
        percentage = round((result_variable_error / (result_variable + result_variable_error))*100, 2)
        return percentage
    
    def _truncate_error_table(self): 
        query = f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.{ERROR}"
        query_job = self.client.query(query)
        results = query_job.result()
        logger.info(f"{ERROR} has been truncated!")
        