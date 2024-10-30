from loguru import logger
from settings import URL_WR, URL_DR

class UrlSelecting:
    def __init__(self):
        self._url = None
        self._route_type = None
        self._client = None

    def _get_valid_input(self):
        if self._url is None or self._route_type is None:
            while True:
                answer = input("Enter 'WR' for WalterRoute or 'DR' for DirectRoute: ").strip()
                if answer == "WR":
                    self._url = URL_WR
                    self._route_type = "WR"
                    self._client = "WRoute"
                    logger.info("WalterRoute has been chosen for the calculation.")
                    break
                elif answer == "DR":
                    self._url = URL_DR
                    self._route_type = "DR"
                    self._client = "DRoute"
                    logger.info("DirectRoute has been chosen for the calculation.")
                    break
                else:
                    logger.info("Invalid input. Please enter 'WR' or 'DR'.")
        return self._url, self._route_type, self._client

    def get_url(self):
        url, _, _ = self._get_valid_input()
        return url

    def get_route_type(self):
        _, route_type, _ = self._get_valid_input()
        return route_type
    
    def get_client(self):
        _, _, client = self._get_valid_input()
        return client