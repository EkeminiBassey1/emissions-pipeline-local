import click
import yaml
from google.oauth2 import service_account

from src.emissions_pipeline.data_transformation.base_coors_data_loading import BaseCoors
from src.emissions_pipeline.data_loading.data_bigquery_upload import update_base_area_code_kilometrierung_table
from src.emissions_pipeline.data_transformation.routen_plz_upload import RoutenPLZ
from src.emissions_pipeline.data_loading.run_sql_query import RunQueries

@click.command()
@click.option("-k", "--key_file", help="Location of the key file to use", type=str, required=True)
def main(key_file):
    project_data = yaml.safe_load(open('config.yaml'))
    data_transformation = BaseCoors()
    run_queries = RunQueries(key_file)
    upload_routen_plz = RoutenPLZ(key_file=key_file, excel_file_path=project_data['input']['file_path_input'])
    
    credentials = service_account.Credentials.from_service_account_file(
        key_file, 
        scopes=["https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/pubsub", 
                "https://www.googleapis.com/auth/cloud-platform"]
        )
    
    run_queries.run_queries_on_bg()
    upload_routen_plz.routen_plz_excle_file_upload()
    data_transformation.loading_bq_table_base_coors()
    update_base_area_code_kilometrierung_table(PROJECT_ID=project_data['project']['project_id'],
                                            DATASET_ID=project_data['project']['dataset_id'],
                                            TABLE_BASE_COORS=project_data['project']['table_base_coors'],
                                            TABLE_BASE_COORS_WR_KILOMETRIERT=project_data['project']['table_base_coors_wr_kilometriert'],
                                            TABLE_ERROR=project_data['project']['table_error'],
                                            CREDENTIALS=credentials,
                                            chunk_size=project_data['project']["batch_size"],
                                            url=project_data['url']["wr_url_walter_route"]
                                            )
    run_queries.run_queries_view()
    data_transformation.transform_bq_table_to_xlsx()

if __name__ == "__main__":
    main()