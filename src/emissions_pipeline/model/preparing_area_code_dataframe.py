import json
from datetime import datetime


def uploading_area_code_dataframe(df, response, index: int):
    response_json = json.loads(response)

    value_routen = response_json.pop('routen')
    value_anzahl = response_json.pop('anzahlGefundenerRouten')

    response_json["ID"] = str(df.iloc[index]["ID"])
    response_json["Land_von"] = str(df.iloc[index]["Land_von"])
    response_json["PLZ_von"] = str(df.iloc[index]["PLZ_von"])
    response_json["Land_nach"] = str(df.iloc[index]["Land_nach"])
    response_json["PLZ_nach"] = str(df.iloc[index]["PLZ_nach"])
    response_json["EventTimeStamp"] = datetime.now()
    response_json["response"] = value_routen
    response_json["anzahlGefundenerRouten"] = value_anzahl

    return response_json