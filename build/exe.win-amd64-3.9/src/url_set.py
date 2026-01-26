from loguru import logger
from settings import URL_WR, URL_DR

# This class named UrlSelecting initializes attributes for URL, route type, and client.
class UrlSelecting:
    def __init__(self):
        self._url = None
        self._route_type = None
        self._client = None

    def _get_valid_input(self):
        """
        The function `_get_valid_input` prompts the user to choose between WalterRoute and DirectRoute,
        setting the URL, route type, and client accordingly.
        :return: The `_get_valid_input` method returns the URL, route type, and client that have been chosen
        for the calculation.
        
        Returns what type of route that has been chosen for the calculation: Walter Route or Direct Route
        URL, routen type and client will be returned 
        """
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
        """
        This function returns the URL for either the calculation of Walter Route or Direct Route.
        :return: The `get_url` method is returning the URL for the calculation of either Walter Route (WR)
        or Direct Route (DR).
        
        Return the url for the calculation Walter Route or Direct Route
        """
        url, _, _ = self._get_valid_input()
        return url

    def get_route_type(self):
        """
        This function retrieves and returns the route type from valid input.
        :return: The `route_type` variable is being returned from the `get_route_type` method.
        """
        _, route_type, _ = self._get_valid_input()
        return route_type
    
    def get_client(self):
        """
        The `get_client` function returns the client from the valid input obtained by calling the
        `_get_valid_input` method.
        :return: The `client` variable is being returned from the `get_client` method.
        """
        _, _, client = self._get_valid_input()
        return client