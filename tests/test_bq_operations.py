import unittest
from unittest.mock import patch, MagicMock
from google.cloud.exceptions import NotFound
from loguru import logger
from src.emissions_pipeline.big_query_table.create_bq_wr_table import BigQuery
from src.emissions_pipeline.big_query_table.check_bq_avail import BQOperations 
from settings import DATASET_ID, BASE_WR_KILOMETRIERT
from google.cloud import bigquery

class TestBQOperations(unittest.TestCase):
    def setUp(self):
        self.bq_operations = BQOperations()
    @patch.object(logger, 'info')
    @patch.object(bigquery.Client, 'get_dataset')
    @patch.object(bigquery.Client, 'delete_dataset')
    def test_check_or_create_dataset_exists(self, mock_delete_dataset, mock_get_dataset, mock_logger):
        mock_get_dataset.return_value = MagicMock()
        self.bq_operations.check_or_create_dataset()
        mock_delete_dataset.assert_called_once_with(DATASET_ID, delete_contents=True, not_found_ok=True)
        mock_logger.assert_any_call(f"Dataset {DATASET_ID} already exists.")
        mock_logger.assert_any_call("Previous dataset has been deleted and a new dataset has been successfully created.")

    @patch.object(logger, 'info')
    @patch.object(bigquery.Client, 'get_dataset', side_effect=NotFound('Dataset not found'))
    @patch.object(bigquery.Client, 'create_dataset')
    def test_check_or_create_dataset_not_exists(self, mock_create_dataset, mock_get_dataset, mock_logger):
        self.bq_operations.check_or_create_dataset()

        mock_create_dataset.assert_called_once()
        mock_logger.assert_any_call(f"Dataset {DATASET_ID} not found. Creating dataset...")
        mock_logger.assert_any_call(f"Dataset {DATASET_ID} created.")

    @patch.object(logger, 'info')
    @patch.object(bigquery.Client, 'get_table')
    def test_check_or_create_table_exists(self, mock_get_table, mock_logger):
        mock_get_table.return_value = MagicMock()
        self.bq_operations.check_or_create_table()
        mock_logger.assert_called_once_with(f"Table {DATASET_ID}.{BASE_WR_KILOMETRIERT} already exists!")

    @patch.object(logger, 'info')
    @patch.object(bigquery.Client, 'get_table', side_effect=NotFound('Table not found'))
    @patch.object(BigQuery, 'create_bigquery_table')
    def test_check_or_create_table_not_exists(self, mock_create_table, mock_get_table, mock_logger):
        self.bq_operations.check_or_create_table()
        mock_create_table.assert_called_once()
        mock_logger.assert_any_call(f"Table {DATASET_ID}.{BASE_WR_KILOMETRIERT} not found. Creating table...")

    @patch.object(BQOperations, 'check_or_create_dataset')
    @patch.object(BQOperations, 'check_or_create_table')
    def test_check_dataset_table(self, mock_check_or_create_table, mock_check_or_create_dataset):
        self.bq_operations.check_dataset_table()
        mock_check_or_create_dataset.assert_called_once()
        mock_check_or_create_table.assert_called_once()

if __name__ == '__main__':
    unittest.main()
