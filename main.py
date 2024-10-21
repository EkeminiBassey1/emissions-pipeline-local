import click
import yaml
from google.oauth2 import service_account

from src.emissions_pipeline.data_transformation.base_coors_data_loading import BaseCoors
from src.emissions_pipeline.data_loading.data_bigquery_upload import update_base_area_code_kilometrierung_table
from src.emissions_pipeline.data_transformation.routen_plz_upload import RoutenPLZ
from src.emissions_pipeline.data_loading.run_sql_query import RunQueries
from src.emissions_pipeline.big_query_table.check_bq_avail import BQOperations
from src.config.settings import PROJECT_ID, DATASET_ID, BASE_COORS, BASE_WR_KILOMETRIERT, ERROR, URL_WR, URL_DR, BATCH_SIZE, CREDENTIALS_PATH


@click.command()
@click.option("-i", "--input_file_path", help="The input file of the area codes", type=str, required=True)
@click.option("-k", "--key_file", help="Location of the key file to use", type=str, required=True)
@click.option("-n", "--name_file", help="The name of the file", type=str, required=True)
@click.option("-o", "--output_file_path", help="Location of where the file will be stored", type=str, required=True)
def main(input_file_path, key_file, output_file_path, name_file):
    data_transformation = BaseCoors(name=name_file, output_path=output_file_path)
    run_queries = RunQueries()
    bq_check = BQOperations()
    upload_routen_plz = RoutenPLZ(excel_file_path=input_file_path)
    
    bq_check.check_dataset_table()
    run_queries.run_queries(use_replacements=True, query_type="main")
    upload_routen_plz.routen_plz_excle_file_upload()
    data_transformation.loading_bq_table_base_coors()
    update_base_area_code_kilometrierung_table(PROJECT_ID=PROJECT_ID,
                                            DATASET_ID=DATASET_ID,
                                            TABLE_BASE_COORS=BASE_COORS,
                                            TABLE_BASE_COORS_WR_KILOMETRIERT=BASE_WR_KILOMETRIERT,
                                            TABLE_ERROR=ERROR,
                                            CREDENTIALS=CREDENTIALS_PATH,
                                            chunk_size=BATCH_SIZE,
                                            url=URL_WR
                                            )
    run_queries.run_queries(use_replacements=True, query_type="view")
    data_transformation.transform_bq_table_to_xlsx()

if __name__ == "__main__":
    main()