from google.cloud import bigquery
from google.oauth2 import service_account

import yaml

class BigQuery:
    def __init__(self, key_file):
        project_data = yaml.safe_load(open('config.yaml'))
        self.project_id = project_data['project']['project_id']
        self.dataset_id = project_data['project']['dataset_id']    
        self.base_coors_wr_k = project_data['project']['table_base_coors_wr_kilometriert']
        credentials = service_account.Credentials.from_service_account_file(
            key_file, 
            scopes=["https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/pubsub", 
                    "https://www.googleapis.com/auth/cloud-platform"]
        )
        self.client = bigquery.Client(credentials=credentials, project=project_data['project']['project_id'])

    def create_bigquery_table(self):

        schema = [
            bigquery.SchemaField("ID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Land_von", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Plz_von", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Land_nach", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Plz_nach", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("eventTimestamp", "TIMESTAMP", mode="NULLABLE"),
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

        self.client.get_table(table_ref) 
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.base_coors_wr_k)

        table = bigquery.Table(table_ref, schema=schema)
        table = self.client.create_table(table)
        print(f"Table {self.project_id}.{self.dataset_id}.{self.base_coors_wr_k} created!")
