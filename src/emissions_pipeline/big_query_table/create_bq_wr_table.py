from google.cloud import bigquery
from loguru import logger
from settings import PROJECT_ID, DATASET_ID, BASE_WR_KILOMETRIERT, CREDENTIALS_PATH


class BigQuery:
    def __init__(self):
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)

    def create_bigquery_table(self):
        """
        The `create_bigquery_table` function in Python creates a BigQuery table with a specified schema.
        """

        schema = [
            bigquery.SchemaField("Client", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Land_von", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Plz_von", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Land_nach", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Plz_nach", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("EventTimeStamp", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("info", "RECORD", mode="NULLABLE", fields=[
                bigquery.SchemaField("totalDistanceInMeters", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("totalStreetDistanceInMeters", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("totalTrainDistanceInMeters", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("totalFerryDistanceInMeters", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("totalTravelTimeInSeconds", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("violated", "BOOLEAN", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("routeSections", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("fromWaypoint", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("address", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("countryIsoCode", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("postalCode", "STRING", mode="NULLABLE"),
                    ]),
                ]),
                bigquery.SchemaField("toWaypoint", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("address", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("countryIsoCode", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("postalCode", "STRING", mode="NULLABLE"),
                    ]),
                ]),
                bigquery.SchemaField("viapoints", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("address", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("countryIsoCode", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("postalCode", "STRING", mode="NULLABLE"),
                    ]),
                ]),
                bigquery.SchemaField("distanceInMeters", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("travelTimeInSeconds", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("travelType", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("walterRouteInfos", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("from", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("isoCountryCode", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("postalCodes", "STRING", mode="REPEATED"),
                    ]),
                    bigquery.SchemaField("to", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("isoCountryCode", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("postalCodes", "STRING", mode="REPEATED"),
                    ]),
                    bigquery.SchemaField("isPreferred", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("prohibitedCountries", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("orgUnit", "STRING", mode="NULLABLE"),
                ]),
                bigquery.SchemaField("segments", "STRING", mode="REPEATED"),
            ]),
            bigquery.SchemaField("tolls", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("isoCountryCode", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("costInEuro", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("distanceInMeters", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("tollType", "STRING", mode="NULLABLE"),
            ]),
        ]

        dataset_ref = self.client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(BASE_WR_KILOMETRIERT)
        try:
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            logger.success(f"Table {PROJECT_ID}.{DATASET_ID}.{BASE_WR_KILOMETRIERT} created!")
        except Exception as e:
            logger.error(f"Table {PROJECT_ID}.{DATASET_ID}.{BASE_WR_KILOMETRIERT} could not be created, because of: {e}")