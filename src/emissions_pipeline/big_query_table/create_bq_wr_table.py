from google.cloud import bigquery
import yaml

class BigQuery:
    def __init__(self, key_file):
        project_data = yaml.safe_load(open('config.yaml'))
        self.project_id = project_data['project']['project_id']
        self.dataset_id = project_data['project']['dataset_id']    
        self.base_coors = project_data['project']['table_base_coors']
        self.routen_plz = project_data['project']['table_routen_plz']
        self.file_path = project_data['output']['file_path']
        self.file_name = project_data['output']['file_name']
        credential_template = project_data['credentials']['credential']
        self.credentials = eval(credential_template.replace("key_path", f'"{key_file}"'))
    
    def create_bigquery_table():
        client = bigquery.Client(project=self.project_id, credentials=self.credentials)

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

        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)

        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Table {self.project_id}.{self.dataset_id}.{self.table_id} created!")
