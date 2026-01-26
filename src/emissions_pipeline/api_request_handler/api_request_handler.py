import asyncio
import json

import aiohttp
from loguru import logger
from requests.auth import HTTPBasicAuth

from src.emissions_pipeline.api_request_handler.apiRequestManager import ApiRequestManager
from src.emissions_pipeline.api_request_handler.preparing_area_code_dataframe import uploading_area_code_dataframe

api_manager = ApiRequestManager()
featureparameter = api_manager.set_featureParameter(
                                                    tollInformation=True,
                                                    polygon=False,
                                                    maneuverEvents=False,
                                                    maneuverEventsLanguage="DE",
                                                    archive=False,
                                                    routeSegments=False
                                                    )

async def send_request_async(session, url, headers, row, max_retries=3, maxRes=50, timeout=10):
    """
    This Python async function sends a POST request with retries and timeouts, handling different
    response statuses.

    :param session: The `session` parameter in the `send_request_async` function is an aiohttp
    ClientSession object that represents a connection pool for making HTTP requests asynchronously. It
    allows you to make HTTP requests using async/await syntax in Python
    :param url: The `url` parameter in the `send_request_async` function is the URL to which the
    asynchronous POST request will be sent. This URL should be the endpoint of the API you are trying to
    communicate with
    :param headers: Headers are typically used to provide additional information to the server when
    making an HTTP request. They can include things like authentication tokens, content type, user-agent
    information, and more. In the context of the `send_request_async` function, headers are passed along
    with the request to provide necessary details to the
    :param row: The `row` parameter in the `send_request_async` function seems to represent a data row
    or object containing information such as 'ID', 'Land_von', 'PLZ_von', 'Land_nach', and 'PLZ_nach'.
    This data is used to set route points and
    :param max_retries: The `max_retries` parameter in the `send_request_async` function determines the
    maximum number of retry attempts that will be made if the initial request fails. If the request
    fails, the function will retry the request up to `max_retries` times before giving up and returning
    a default response containing, defaults to 3 (optional)
    :param maxRes: The `maxRes` parameter in the `send_request_async` function represents the maximum
    number of results to be included in the request payload. This parameter is used when creating the
    request body to specify the maximum number of results that should be returned by the API in response
    to the request, defaults to 50 (optional)
    :param timeout: The `timeout` parameter in the `send_request_async` function specifies the maximum
    time, in seconds, that the function will wait for a response from the server before considering the
    request as timed out. If the response is not received within the specified timeout period, an
    `asyncio.TimeoutError` exception, defaults to 10 (optional)
    :return: The function `send_request_async` is returning either the response text if the status is
    200, or a dictionary containing the details of the row if all retries fail.
    """
    for attempt in range(max_retries):
        try:
            routenpunkte = api_manager.set_routenpunkte_zonenpunkt(row, 'Land_von', 'PLZ_von', 'Land_nach', 'PLZ_nach')
            request_data = api_manager.create_request_body(routenpunkte, featureparameter, maxResults=maxRes)
            request_raw = api_manager.convert_request(request_data)
            json_payload = json.loads(request_raw)

            auth = aiohttp.BasicAuth("rout-test", "SUvrfpMtYjtZ")

            headers_II = {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }

            async with session.post(url, json=json_payload, headers=headers_II, auth=auth, timeout=timeout) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.info(f"Request for row {row.name} failed with status code {response.status}")
        except asyncio.TimeoutError:
            logger.info(f"Timeout in request for row {row.name}")

        delay = 2 ** attempt
        logger.info(f"Retrying request for row {row.name} after {delay} seconds...")
        await asyncio.sleep(delay)

    logger.info(f"All retries failed for row {row.name}")
    return {
        "ID": row['ID'],
        "Land_von": row['Land_von'],
        "PLZ_von": row["PLZ_von"],
        "Land_nach": row["Land_nach"],
        "PLZ_nach": row["PLZ_nach"]
    }

async def send_requests_async(df, url, headers):
    successful_responses = []
    failed_requests = []

    async with aiohttp.ClientSession() as session:
        tasks = [send_request_async(session, url, headers, row) for _, row in df.iterrows()]
        responses = await asyncio.gather(*tasks)

    for index, response in enumerate(responses):
        if response is not None:
            if isinstance(response, dict):
                failed_requests.append(response)
                logger.error(f"Request for row {index} failed")
            else:
                response_json = uploading_area_code_dataframe(df, response, index)
                successful_responses.append(response_json)
    return successful_responses, failed_requests