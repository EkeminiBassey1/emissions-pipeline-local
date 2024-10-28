from google.cloud import bigquery
from google.oauth2 import service_account
from loguru import logger
import yaml
from settings import PROJECT_ID, DATASET_ID, BASE_COORS, BASE_WR_KILOMETRIERT, ERROR, URL_WR, URL_DR, BATCH_SIZE, CREDENTIALS_PATH


class BigQuery:
    def __init__(self):
        self.client = bigquery.Client(credentials=CREDENTIALS_PATH, project=PROJECT_ID)

    def create_bigquery_table(self):

        schema = [
            bigquery.SchemaField("ID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Land_von", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Plz_von", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Land_nach", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Plz_nach", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("eventTimestamp", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("response", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("rank", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("walterRoutenInfos", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("walterRoutenId", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("istBevorzugt", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("laendersperren", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("beschreibung", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("orgEinheit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("geografieVon", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("land", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("plzZonen", "STRING", mode="REPEATED"),
                    ]),
                    bigquery.SchemaField("geografieNach", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("land", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("plzZonen", "STRING", mode="REPEATED"),
                    ])
                ]),
                bigquery.SchemaField("routenPunkte", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("laufendeNr", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("typ", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("ursprungKoordinate", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("x", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("y", "FLOAT", mode="NULLABLE"),
                    ]),
                    bigquery.SchemaField("berechneteKoordinate", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("x", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("y", "FLOAT", mode="NULLABLE"),
                    ]),
                    bigquery.SchemaField("fahrzeit", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("distanzTyp", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("distanz", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("polygon", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("maut", "RECORD", mode="REPEATED", fields=[
                        bigquery.SchemaField("landCode", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("landName", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("kosten", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("strecke", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("tollTypes", "RECORD", mode="REPEATED", fields=[
                            bigquery.SchemaField("tollType", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("strecke", "INTEGER", mode="NULLABLE"),
                            bigquery.SchemaField("kosten", "INTEGER", mode="NULLABLE"),
                        ])
                    ]),
                    bigquery.SchemaField("viaPunkte", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("land", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("ort", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("plz", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("mnemoKuerzel", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("ursprungTyp", "STRING", mode="NULLABLE")
                ]),
                bigquery.SchemaField("strassenDistanzGesamt", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("faehreDistanzGesamt", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("bahnDistanzGesamt", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("mautGesamt", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("landCode", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("landName", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("kosten", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("strecke", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("tollTypes", "RECORD", mode="REPEATED", fields=[
                        bigquery.SchemaField("tollType", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("strecke", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("kosten", "INTEGER", mode="NULLABLE"),
                    ])
                ]),
                bigquery.SchemaField("manoeuvres", "INTEGER", mode="REPEATED"),
            ]),
            bigquery.SchemaField("anzahlGefundenerRouten", "INTEGER", mode="NULLABLE")
        ]

        dataset_ref = self.client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(BASE_WR_KILOMETRIERT)
        try: 
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            logger.success(f"Table {PROJECT_ID}.{DATASET_ID}.{BASE_WR_KILOMETRIERT} created!")
        except: 
            logger.error(f"Table {PROJECT_ID}.{DATASET_ID}.{BASE_WR_KILOMETRIERT} could not be created!")