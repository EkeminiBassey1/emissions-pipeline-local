import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import hashlib
from loguru import logger
from google.cloud import bigquery
from src.emissions_pipeline.data_transformation.base_coors_uploading import loading_bq_table_base_coors
from settings import PROJECT_ID, DATASET_ID, BASE_COORS, CREDENTIALS_PATH

class TestLoadingBQTableBaseCoors(unittest.TestCase):

    @patch('src.emissions_pipeline.data_transformation.base_coors_uploading.bigquery.Client')
    @patch('src.emissions_pipeline.data_transformation.base_coors_uploading.read_gbq')
    @patch.object(logger, 'success')
    def test_loading_bq_table_base_coors(self, mock_logger_success, mock_read_gbq, mock_bigquery_client):
        sample_data = {
            'column1': [1, 2],
            'column2': ['A', 'B']
        }
        df_sample = pd.DataFrame(sample_data)
        mock_read_gbq.return_value = df_sample
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bigquery_client.return_value.load_table_from_dataframe.return_value = mock_job
        
        loading_bq_table_base_coors('test_table')

        mock_read_gbq.assert_called_once_with(
            f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.test_table`",
            project_id=PROJECT_ID,
            dialect="standard"
        )

        mock_bigquery_client.return_value.load_table_from_dataframe.assert_called_once()
        loaded_dataframe = mock_bigquery_client.return_value.load_table_from_dataframe.call_args[0][0]
        self.assertIn('ID', loaded_dataframe.columns)

        expected_ids = [
            hashlib.md5(f"{row['column1']}{row['column2']}".encode()).hexdigest() for index, row in df_sample.iterrows()
        ]
        loaded_ids = loaded_dataframe['ID'].tolist()
        
        self.assertListEqual(loaded_ids, expected_ids)

        mock_logger_success.assert_called_once_with(f'Data has been successfully loaded into {BASE_COORS}!')

if __name__ == '__main__':
    unittest.main()
