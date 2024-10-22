import os
import yaml
import json
from google.oauth2 import service_account
import os
import glob

def load_yaml_config(yaml_file):
    with open(yaml_file, 'r') as file:
        return yaml.safe_load(file)

def load_env_variables(key_file, project_id, dataset_id, base_coors, table_base_coors_wr_kilometriert, table_error, routen_plz, table_view, url_wr, url_dr, batch_size):
    credentials = service_account.Credentials.from_service_account_file(
            key_file,
            scopes=[
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/pubsub", 
                "https://www.googleapis.com/auth/cloud-platform"
            ]
        )
    return {
        "PROJECT_ID": project_id,
        "DATASET_ID": dataset_id,
        "TABLE_BASE_COORS": base_coors, 
        "TABLE_BASE_COORS_WR_KILOMETRIERT": table_base_coors_wr_kilometriert, 
        "TABLE_ERROR": table_error,
        "ROUTEN_PLZ":routen_plz,
        "TABLE_VIEW": table_view,
        "URL_WR": url_wr,
        "URL_DR": url_dr, 
        "BATCH_SIZE": batch_size,
        "CREDENTIALS": credentials
    }

def get_single_json_filename(folder_path):
    json_files = glob.glob(os.path.join(folder_path, "*.json"))
    
    if len(json_files) == 0:
        raise FileNotFoundError("No JSON file found in the folder.")
    elif len(json_files) > 1:
        raise ValueError("More than one JSON file found in the folder.")
    
    json_file = json_files[0]    
    return json_file

key_file = get_single_json_filename('key_file')
yaml_config = load_yaml_config(os.path.join(os.path.dirname(__file__), 'config.yaml'))
env_config = load_env_variables(
    key_file=key_file, 
    project_id=yaml_config["project"]["project_id"], 
    dataset_id=yaml_config["project"]["dataset_id"], 
    base_coors=yaml_config["project"]["table_base_coors"], 
    table_base_coors_wr_kilometriert=yaml_config["project"]["table_base_coors_wr_kilometriert"], 
    table_error=yaml_config["project"]["table_error"],
    routen_plz=yaml_config["project"]["table_routen_plz"],  
    table_view=yaml_config["project"]["table_view"], 
    url_wr=yaml_config["url"]["wr_url_walter_route"],
    url_dr=yaml_config["url"]["wr_url_direct_route"],  
    batch_size=yaml_config["project"]["batch_size"]
    )
CONFIG = {**yaml_config, **env_config}

PROJECT_ID = CONFIG.get("PROJECT_ID")
DATASET_ID = CONFIG.get("DATASET_ID")
BASE_COORS = CONFIG.get("TABLE_BASE_COORS")
BASE_WR_KILOMETRIERT = CONFIG.get("TABLE_BASE_COORS_WR_KILOMETRIERT")
ERROR = CONFIG.get("TABLE_ERROR")
ROUTEN_PLZ=CONFIG.get("ROUTEN_PLZ")
TABLE_VIEW = CONFIG.get("TABLE_VIEW")
URL_WR = CONFIG.get("URL_WR")
URL_DR = CONFIG.get("URL_DR")
BATCH_SIZE = CONFIG.get("BATCH_SIZE")
CREDENTIALS_PATH = CONFIG.get("CREDENTIALS_PATH")