import json
from datetime import datetime


def uploading_area_code_dataframe(df, response, index: int):
    """
    This Python function updates a JSON response with data from a DataFrame based on a specified index.
    
    :param df: A pandas DataFrame containing area code information such as ID, Land_von, PLZ_von,
    Land_nach, and PLZ_nach
    :param response: The function `uploading_area_code_dataframe` takes a DataFrame `df`, a response in
    JSON format, and an index as input parameters. It then extracts specific values from the response
    JSON, modifies it by adding additional key-value pairs from the DataFrame at the specified index,
    and returns the updated JSON object
    :param index: The `index` parameter in the `uploading_area_code_dataframe` function is used to
    specify the row index of the DataFrame `df` that you want to access and update with the information
    from the `response`. It helps in identifying the specific row in the DataFrame where the data should
    be uploaded
    :type index: int
    :return: The function `uploading_area_code_dataframe` returns a modified JSON object with additional
    key-value pairs based on the input DataFrame `df`, response data, and the specified index.
    """
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