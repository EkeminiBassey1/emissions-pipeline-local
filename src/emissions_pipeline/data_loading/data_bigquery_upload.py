import asyncio

import pandas as pd
from loguru import logger
from pandas_gbq import read_gbq
import json
from google.cloud import bigquery
from settings import PROJECT_ID, DATASET_ID, BASE_COORS, BASE_WR_KILOMETRIERT, ERROR, CREDENTIALS_PATH
from src.emissions_pipeline.api_request_handler.api_request_handler import send_requests_async


class BigQUpload:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS_PATH)
        self.query_test = f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{BASE_COORS}"
        self.destination_table_err = f"{PROJECT_ID}.{DATASET_ID}.{ERROR}"
        self.destination_table = f"{PROJECT_ID}.{DATASET_ID}.{BASE_WR_KILOMETRIERT}"

    def update_base_area_code_kilometrierung_table(self, url: str, batch_size: int, client_type:str, offset: int = 0):
        """
        The function `update_base_area_code_kilometrierung_table` uploads data from a dataframe to BigQuery
        in batches, handling successful and failed requests accordingly.

        :param base_coors_wr_kilometriert: The `base_coors_wr_kilometriert` parameter in the
        `update_base_area_code_kilometrierung_table` function is a string representing the name of a table
        in BigQuery where the base coordinates with kilometers are stored. This table will be used as the
        destination table for loading
        :type base_coors_wr_kilometriert: str
        :param url: The `url` parameter in the `update_base_area_code_kilometrierung_table` function is a
        string that represents the URL used for making requests in the code. It is passed to the function to
        specify the endpoint where the requests will be sent
        :type url: str
        :param batch_size: The `batch_size` parameter in the `update_base_area_code_kilometrierung_table`
        function determines the number of rows from the dataframe that will be processed and uploaded to
        BigQuery in each iteration of the loop. It helps in managing the amount of data being processed at a
        time to prevent
        :type batch_size: int
        :param client_type: The `client_type` parameter in the `update_base_area_code_kilometrierung_table`
        function is used to specify the type of client for which the data is being updated in the BigQuery
        table. It is a string parameter that helps identify or categorize the client associated with the
        data being
        :type client_type: str
        :param offset: The `offset` parameter in the `update_base_area_code_kilometrierung_table` function
        is used to specify the starting index from where the data should be processed in batches. It helps
        in iterating over the dataframe in chunks of `batch_size` starting from the specified offset,
        defaults to 0
        :type offset: int (optional)
        """
        headers = {
            "X-FWD": url,
            "X-AUTH": "wT6vNTkzKXH8E7jOfA5ay4cHGwoJPhOc2XS7UyhVWnQ",
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
            "Connection": "close",
        }

        df = read_gbq(self.query_test, project_id=PROJECT_ID, dialect="standard")

        loop = asyncio.get_event_loop()
        job_config = bigquery.LoadJobConfig(create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_APPEND")

        logger.info("Start uploading dataframe to Bigquery...")
        while True:
            df_temp = df[offset: offset + batch_size]

            successful_responses, failed_requests = loop.run_until_complete(
                send_requests_async(df_temp, url, headers))
            failed_df = pd.DataFrame(failed_requests)
            df_json_responses = pd.DataFrame(successful_responses)
            df_json_responses["Client"] = client_type

            col_name = next((c for c in ["RouteSections", "routeSections"] if c in df_json_responses.columns), None)

            if col_name is not None:
                df_json_responses[col_name] = df_json_responses[col_name].apply(
                    lambda x: json.dumps(x) if x is not None else None
                )

                df_json_responses.rename(columns={col_name: "routeSections"}, inplace=True)
            else:
                df_json_responses["routeSections"] = None
                logger.warning("Spalte RouteSections wurde im Response nicht gefunden - erstelle leere Spalte.")

            try:
                job = self.client.load_table_from_dataframe(df_json_responses, self.destination_table, job_config=job_config)
                job.result()
                logger.success(
                    f"Data has been loaded successfully into {BASE_WR_KILOMETRIERT}")
            except Exception as e:
                logger.error(f"Error writing to BigQuery table: {e}")

            try:
                job = self.client.load_table_from_dataframe(
                    failed_df, self.destination_table_err, job_config=job_config)
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

    def sanitize_for_bigquery(self, sections):
        if not isinstance(sections, list):
            return sections

        for section in sections:
            walter = section.get("walterRouteInfos")
            if isinstance(walter, dict):
                for direction_key in ["from", "to"]:
                    direction = walter.get(direction_key)
                    if isinstance(direction, dict) and "postalCodes" in direction:
                        direction["postalCodes"] = [str(pc) for pc in direction["postalCodes"]]

            if "viapoints" not in section or section["viapoints"] is None:
                section["viapoints"] = []
            else:
                for vp in section["viapoints"]:
                    addr = vp.get("address")
                    if isinstance(addr, dict) and "postalCode" in addr:
                        addr["postalCode"] = str(addr["postalCode"])
        return sections