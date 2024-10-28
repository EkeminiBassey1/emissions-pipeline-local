import json
import os
from datetime import datetime

import requests
from loguru import logger

from src.emissions_pipeline.model.request_body import RequestBody, FeatureParameter
from src.emissions_pipeline.model.request_body import Routenpunkte_zonenpunkt, Routenpunkte_koordinaten
from src.emissions_pipeline.model.request_body import Koordinaten, Zonenpunkte
from settings import URL_WR, URL_DR


class ApiRequestManager:
    def __init__(self):
        pass

    def set_featureParameter(self, polygon: bool = False, fahrzeit: bool = True, maut: bool = True) -> dict:
        featureParameter_dict = {
            "featureParameter": vars(FeatureParameter(polygon=polygon, fahrzeit=fahrzeit, maut=maut))}
        return featureParameter_dict

    def _key_remove(self, liste: list, key_remove):
        for my_dict in liste:
            if key_remove in my_dict:
                my_dict.pop(key_remove, None)

    def set_routenpunkte_zonenpunkt(self, row, column_1: str, column_2: str, column_3: str, column_4: str):
        routenpunkte_list = [
            vars(Routenpunkte_zonenpunkt(typ="WegPunkt",
                                         zonenpunkt=Zonenpunkte(land=str(row[column_1]),
                                                                plzZone=str(row[column_2])))),
            vars(Routenpunkte_zonenpunkt(typ="WegPunkt",
                                         zonenpunkt=Zonenpunkte(land=str(row[column_3]),
                                                                plzZone=str(row[column_4]))))
        ]
        return routenpunkte_list

    def set_routenpunkte_koordinaten(self, row, column_1: str, column_2: str, column_3: str, column_4: str):
        routenpunkte_list = [
            vars(Routenpunkte_koordinaten(typ="WegPunkt",
                                          koordinaten=Koordinaten(x=float(row[column_1]), y=float(row[column_2])))),
            vars(Routenpunkte_koordinaten(typ="WegPunkt",
                                          koordinaten=Koordinaten(x=float(row[column_3]), y=float(row[column_4])))),
        ]
        return routenpunkte_list

    def set_org_einheit(self, dataframe):
        if isinstance(dataframe["ORGEINHEIT"][0], int):
            pass
        else:
            dataframe["ORGEINHEIT"][0] = None
        pass

    def create_request_body(self, routenpunkte: list, featureparameter: dict, archive: bool = False,
                            org_einheit: int = None, maxResults: int = 1, laendersperren: bool = None,
                            manoeuvres: bool = None, routensegmente: bool = None):

        feature_parameter_instance = FeatureParameter(
            **featureparameter['featureParameter'])

        request_body = RequestBody(
            archive=archive,
            routenpunkte=routenpunkte,
            orgeinheit=org_einheit,
            maxResults=maxResults,
            laendersperren=laendersperren,
            manoeuvres=manoeuvres,
            routensegmente=routensegmente,
            featureParameter=feature_parameter_instance,
        )
        return request_body

    def set_header_with_url(self, url):
        headers = {
            "Content-type": "application/json",
            "X-FWD": url,
            "X-AUTH": os.getenv("XAUTH")
        }
        return headers

    def get_response(self, ct_url, headers, request_data):
        request_data_json = json.dumps(request_data.dict())
        response = requests.post(
            ct_url, json=request_data_json, headers=headers)
        return response

    def save_request_bucket(self, client_storage, bucket_name):
        blob = client_storage.get_bucket(bucket_name, location="EU").blob(
            f"request_{datetime.now()}.txt")
        blob.content_type = 'text/plain'

    def convert_request(self, request_data):
        request_data_json = json.dumps(request_data.dict())
        return request_data_json

    def set_MaxResults(self, url):
        if url == URL_WR:
            maxResults = 50
            return maxResults
        elif url == URL_DR:
            maxResults = 2
            return maxResults
