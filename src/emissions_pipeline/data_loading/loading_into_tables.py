from src.emissions_pipeline.data_transformation.converting_bq_excel import BQTransformation
from src.emissions_pipeline.data_loading.data_bigquery_upload import BigQUpload
from src.emissions_pipeline.data_transformation.routen_plz_upload import RoutenPLZ
from src.emissions_pipeline.data_loading.run_sql_query import RunQueries
from src.emissions_pipeline.data_loading.data_re_loading import ReRun
from src.emissions_pipeline.big_query_table.check_bq_avail import BQOperations
from src.emissions_pipeline.api_request_handler.url_set import UrlSelecting
from src.emissions_pipeline.data_transformation.base_coors_uploading import loading_bq_table_base_coors
from settings import TABLE_VIEW, DIRECT_ROUTE_VIEW, BASE_WR_KILOMETRIERT, ROUTEN_PLZ, BATCH_SIZE
from loguru import logger


class DataPrep:
    def __init__(self, input_file_path, output_file_path):
        self.data_transformation = BQTransformation(
            output_path=output_file_path)
        self.run_queries = RunQueries()
        self.bq_check = BQOperations()
        self.uploading_to_bq = BigQUpload()
        self.re_run_errors = ReRun()
        self.url_choosing = UrlSelecting()
        self.upload_routen_plz = RoutenPLZ(excel_file_path=input_file_path)

    def loading_into_tables(self):
        self._step_data_preperation()
        self._step_data_kilometrierung()
        self._step_creating_excel_files()

    def _step_data_preperation(self):
        logger.info("The step data preparation...")
        self.bq_check.check_dataset_table()
        self.run_queries.run_queries(use_replacements=True, query_type="main")
        self.upload_routen_plz.routen_plz_excle_file_upload()
        loading_bq_table_base_coors(TABLE=ROUTEN_PLZ)
        logger.success("Step data preparation has been completed!")

    def _step_data_kilometrierung(self):
        logger.info("Step Kilometrierung...")
        self.uploading_to_bq.update_base_area_code_kilometrierung_table(
            base_coors_wr_kilometriert=BASE_WR_KILOMETRIERT, url=self.url_choosing.get_url(), batch_size=BATCH_SIZE)
        self.re_run_errors.re_run_failed_requests(url=self.url_choosing.get_url())
        logger.success("Kilometrierung has been completed!")

    def _step_creating_excel_files(self):
        logger.info("Step creating view and excel file...")
        if self.url_choosing.get_route_type() == "WR":
            logger.info("Creating view for WalterRoute...")
            self.run_queries.run_queries(use_replacements=True, query_type="view")
            self.data_transformation.transform_bq_table_to_xlsx(table=TABLE_VIEW)
        elif self.url_choosing.get_route_type() == "DR":
            logger.info("Creating view for DirectRoute...")
            self.run_queries.create_directRoute_view()
            self.data_transformation.transform_bq_table_to_xlsx(table=DIRECT_ROUTE_VIEW)
        logger.success("View and excel files haven been created!")