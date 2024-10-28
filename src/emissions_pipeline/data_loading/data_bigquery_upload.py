import asyncio

import pandas as pd
from loguru import logger
from pandas_gbq import read_gbq
from google.cloud import bigquery
from settings import PROJECT_ID, DATASET_ID, BASE_COORS, BASE_WR_KILOMETRIERT, ERROR, CREDENTIALS_PATH
from src.emissions_pipeline.model.api_request_handler import send_requests_async


class BigQUpload:
    def __init__(self):
        pass

    def update_base_area_code_kilometrierung_table(self, base_coors_wr_kilometriert: str, url: str, batch_size: int, offset: int = 0):
        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{base_coors_wr_kilometriert}"
        destination_table_err = f"{PROJECT_ID}.{DATASET_ID}.{ERROR}"
        headers = {
            "X-FWD": url,
            "X-AUTH": "wT6vNTkzKXH8E7jOfA5ay4cHGwoJPhOc2XS7UyhVWnQ",
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
            "Connection": "close",
        }

        client = bigquery.Client(
            project=PROJECT_ID, credentials=CREDENTIALS_PATH)

        query_test = f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{BASE_COORS}"

        df = read_gbq(query_test, project_id=PROJECT_ID, dialect="standard")

        loop = asyncio.get_event_loop()
        job_config = bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_APPEND")

        logger.info("Start uploading dataframe to Bigquery...")
        while True:
            df_temp = df[offset: offset + batch_size]

            successful_responses, failed_requests = loop.run_until_complete(
                send_requests_async(df_temp, url, headers))
            failed_df = pd.DataFrame(failed_requests)
            df_json_responses = pd.DataFrame(successful_responses)

            try:
                job = client.load_table_from_dataframe(
                    df_json_responses, destination_table, job_config=job_config)
                job.result()
                logger.success(
                    f"Data has been loaded successfully into {BASE_WR_KILOMETRIERT}")
            except Exception as e:
                logger.error(f"Error writing to BigQuery table: {e}")

            try:
                job = client.load_table_from_dataframe(
                    failed_df, destination_table_err, job_config=job_config)
                job.result()
                logger.success(
                    f"Data has been loaded successfully into {ERROR}")
            except Exception as e:
                if failed_df.empty:
                    logger.info("No errors have been logged...")
                else:
                    logger.error(f"Error writing to BigQuery table: {e}")

            if df_temp.empty:
                logger.success("DataFrame is empty. Exiting the loop.")
                break
            else:
                logger.success(
                    "DataFrame is not empty. Continuing the loop...")

            logger.success(
                f"Dataframe from {offset} to {offset + batch_size} has been completed!")
            offset += batch_size
        logger.success("Calculations have been uploaded successfully!")
