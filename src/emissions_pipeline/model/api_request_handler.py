import asyncio
import json

import aiohttp
from loguru import logger

from src.emissions_pipeline.data_intergration.apiRequestManager import ApiRequestManager
from src.emissions_pipeline.model.preparing_area_code_dataframe import uploading_area_code_dataframe 

api_manager = ApiRequestManager()
featureparameter = api_manager.set_featureParameter(maut=True)

async def send_request_async(session, url, headers, row, max_retries=3, timeout=10):
    for attempt in range(max_retries):
        try:
            routenpunkte = api_manager.set_routenpunkte_zonenpunkt(row, 'Land_von', 'PLZ_von', 'Land_nach', 'PLZ_nach')
            maxResults = api_manager.set_MaxResults(url=url)
            request_data = api_manager.create_request_body(routenpunkte, featureparameter, maxResults=maxResults)
            
            json_payload = json.loads(api_manager.convert_request(request_data))

            async with session.post(url, json=json_payload, headers=headers, timeout=timeout) as response:
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