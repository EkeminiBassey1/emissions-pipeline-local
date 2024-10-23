from src.emissions_pipeline.data_transformation.converting_bq_excel import BQLoading
from src.emissions_pipeline.data_loading.data_bigquery_upload import BigQUpload
from src.emissions_pipeline.data_transformation.routen_plz_upload import RoutenPLZ
from src.emissions_pipeline.data_loading.run_sql_query import RunQueries
from src.emissions_pipeline.data_loading.data_re_loading import ReRun
from src.emissions_pipeline.big_query_table.check_bq_avail import BQOperations
from src.emissions_pipeline.data_transformation.base_coors_uploading import loading_bq_table_base_coors
from src.config.settings import URL_WR, URL_DR, BASE_WR_KILOMETRIERT, ROUTEN_PLZ, BATCH_SIZE
from loguru import logger

def loading_into_tables(input_file_path, output_file_path, name_file):
    data_transformation = BQLoading(name=name_file, output_path=output_file_path)
    run_queries = RunQueries()
    bq_check = BQOperations()
    uploading_to_bq = BigQUpload()
    re_run_errors = ReRun()
    upload_routen_plz = RoutenPLZ(excel_file_path=input_file_path)
    
    url_wr_dr_choice = _get_valid_input()
    
    bq_check.check_dataset_table()
    run_queries.run_queries(use_replacements=True, query_type="main")
    upload_routen_plz.routen_plz_excle_file_upload()
    loading_bq_table_base_coors(TABLE=ROUTEN_PLZ)
    uploading_to_bq.update_base_area_code_kilometrierung_table(base_coors_wr_kilometriert=BASE_WR_KILOMETRIERT, batch_size=BATCH_SIZE ,url=url_wr_dr_choice)
    re_run_errors.re_run_failed_requests(url=url_wr_dr_choice)
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