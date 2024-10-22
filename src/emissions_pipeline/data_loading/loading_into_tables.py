from src.emissions_pipeline.data_transformation.base_coors_data_loading import BaseCoors
from src.emissions_pipeline.data_loading.data_bigquery_upload import update_base_area_code_kilometrierung_table
from src.emissions_pipeline.data_transformation.routen_plz_upload import RoutenPLZ
from src.emissions_pipeline.data_loading.run_sql_query import RunQueries
from src.emissions_pipeline.big_query_table.check_bq_avail import BQOperations
from src.config.settings import PROJECT_ID, DATASET_ID, BASE_COORS, BASE_WR_KILOMETRIERT, ERROR, URL_WR, URL_DR, BATCH_SIZE, CREDENTIALS_PATH
from loguru import logger

def loading_into_tables(input_file_path, output_file_path, name_file):
    data_transformation = BaseCoors(name=name_file, output_path=output_file_path)
    run_queries = RunQueries()
    bq_check = BQOperations()
    upload_routen_plz = RoutenPLZ(excel_file_path=input_file_path)
    
    url_wr_dr_choice = _get_valid_input()
    
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
                                            url=url_wr_dr_choice
                                            )
    run_queries.run_queries(use_replacements=True, query_type="view")
    data_transformation.transform_bq_table_to_xlsx()

def _get_valid_input():
    while True:
        answer = input("Enter 'WR' for WalterRoute or 'DR' for DirectRoute: ").strip()
        if answer in ["WR", "DR"]:
            if answer == "WR":
                url = URL_WR
                logger.info("WalterRoute has been chosen for the calculation.")
                return url
            elif answer == "DR":
                url = URL_DR
                logger.info("DirectRoute has been chosen for the calculation.")
                return url
        else:
            print("Invalid input. Please enter 'WR' or 'DR'.")