import unittest
from unittest.mock import patch
from loguru import logger
from src.emissions_pipeline.api_request_handler.url_set import UrlSelecting  
from settings import URL_WR, URL_DR

class TestUrlSelecting(unittest.TestCase):
    def setUp(self):
        self.selector = UrlSelecting()

    @patch('builtins.input', side_effect=['WR'])
    @patch.object(logger, 'info')
    def test_get_valid_input_with_wr(self, mock_logger, mock_input):
        # Test 'WR' input
        url, route_type, client = self.selector._get_valid_input()
        self.assertEqual(url, URL_WR)
        self.assertEqual(route_type, 'WR')
        self.assertEqual(client, 'WRoute')
        mock_logger.assert_called_with("WalterRoute has been chosen for the calculation.")

    @patch('builtins.input', side_effect=['DR'])
    @patch.object(logger, 'info')
    def test_get_valid_input_with_dr(self, mock_logger, mock_input):
        url, route_type, client = self.selector._get_valid_input()
        self.assertEqual(url, URL_DR)
        self.assertEqual(route_type, 'DR')
        self.assertEqual(client, 'DRoute')
        mock_logger.assert_called_with("DirectRoute has been chosen for the calculation.")

    @patch('builtins.input', side_effect=['invalid', 'WR'])
    @patch.object(logger, 'info')
    def test_get_valid_input_with_invalid_then_wr(self, mock_logger, mock_input):
        url, route_type, client = self.selector._get_valid_input()
        self.assertEqual(url, URL_WR)
        self.assertEqual(route_type, 'WR')
        self.assertEqual(client, 'WRoute')
        mock_logger.assert_any_call("Invalid input. Please enter 'WR' or 'DR'.")
        mock_logger.assert_any_call("WalterRoute has been chosen for the calculation.")

    @patch('builtins.input', side_effect=['WR'])
    def test_get_url(self, mock_input):
        url = self.selector.get_url()
        self.assertEqual(url, URL_WR)

    @patch('builtins.input', side_effect=['DR'])
    def test_get_route_type(self, mock_input):
        route_type = self.selector.get_route_type()
        self.assertEqual(route_type, 'DR')

    @patch('builtins.input', side_effect=['WR'])
    def test_get_client(self, mock_input):
        client = self.selector.get_client()
        self.assertEqual(client, 'WRoute')

if __name__ == '__main__':
    unittest.main()