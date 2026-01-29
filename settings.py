import os
import sys
import yaml
from google.oauth2 import service_account
import json
import pyfiglet
from colorama import init, Fore
from loguru import logger

init()

def validate_and_correct_name(user_input):
    special_cases = {
        'ä': 'ae', 'ö': 'oe', 'ü': 'ue',
        'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue',
        'ß': 'ss'
    }

    for char, replacement in special_cases.items():
        user_input = user_input.replace(char, replacement)

    corrected_name = ''.join(char for char in user_input if char.isalpha() or char.isspace())

    if not corrected_name.strip():
        return None

    return corrected_name.strip()

def _get_valid_name():
    while True:
        user_name_dataset = input("Please, insert a dataset name: ").lower()
        corrected_name = validate_and_correct_name(user_name_dataset)

        if corrected_name:
            logger.info(f"Dataset will be named: emission_{corrected_name}")
            return corrected_name
        else:
            logger.error("Invalid name. Please enter a valid name using only letters and spaces.")

def display_banner():
    ascii_banner = pyfiglet.figlet_format("Emissions Pipeline", font="doom")

    print(Fore.GREEN + ascii_banner + Fore.RESET)

    print(Fore.YELLOW + "Emissions Pipeline - Processing Data..." + Fore.RESET)
    print(Fore.CYAN + "Version: 1.0.0 | Status: Running | Current Process: Carbon Capture" + Fore.RESET)
    print(Fore.CYAN + "Next Step: Data Analysis | Time Remaining: Estimating..." + Fore.RESET)

def _load_env_variables(base_coors, table_base_coors_wr_kilometriert, table_error, routen_plz, table_view, url_wr, url_dr, error_rate_toleration, batch_size, folder_name, excel_file_name, direct_route_view, bucket_name, file_name, user, password):
    display_banner()
    print("Starting emissions data processing...\n")

    key_file_path = input("Please, insert the path...: ").strip().replace("'", "").replace('"', "")
    user_name_dataset = _get_valid_name()

    with open(key_file_path, "r") as file:
        file_content = file.read()

    service_account_key_file = json.loads(file_content)

    credentials = service_account.Credentials.from_service_account_file(
        key_file_path,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/pubsub",
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )
    return {
        "PROJECT_ID": service_account_key_file["project_id"],
        "DATASET_ID": f"emission_{user_name_dataset}",
        "TABLE_BASE_COORS": base_coors,
        "TABLE_BASE_COORS_WR_KILOMETRIERT": table_base_coors_wr_kilometriert,
        "TABLE_ERROR": table_error,
        "ROUTEN_PLZ": routen_plz,
        "TABLE_VIEW": table_view,
        "URL_WR": url_wr,
        "URL_DR": url_dr,
        "BATCH_SIZE": batch_size,
        "FOLDER_NAME": folder_name,
        "EXCEL_FILE_NAME": excel_file_name,
        "ERROR_RATE_TOL": error_rate_toleration,
        "CREDENTIALS_PATH": credentials,
        "DIRECT_ROUTE_VIEW": direct_route_view,
        "BUCKET_NAME": bucket_name,
        "FILE_NAME": file_name,
        "USER": user,
        "PASSWORD": password
    }

def _get_resource_path(relative_path):
    """Get the absolute path to a resource, works for dev and PyInstaller."""
    if hasattr(sys, '_MEIPASS'):  # PyInstaller sets this attribute
        base_path = sys._MEIPASS
    else:
        base_path = os.path.abspath(".")  # Use current directory in development
    return os.path.join(base_path, relative_path)

def _load_yaml_config(yaml_file):
    with open(yaml_file, 'r', encoding="utf-8") as file:
        return yaml.safe_load(file)

# Check if we are running as a bundled app or script
if getattr(sys, 'frozen', False):
    # Running as a PyInstaller executable
    base_path = sys._MEIPASS
else:
    # Running as a script
    base_path = os.path.dirname(__file__)

# Path to config.yaml in both cases
config_path = _get_resource_path('config.yaml')

# Load the YAML config from the correct location
yaml_config = _load_yaml_config(config_path)

# Path to sql_queries directory
sql_queries_path = _get_resource_path('src/util/sql_queries')
logger.debug(f"SQL Queries Path: {sql_queries_path}")

env_config = _load_env_variables(
    base_coors=yaml_config["project"]["table_base_coors"],
    table_base_coors_wr_kilometriert=yaml_config["project"]["table_base_coors_wr_kilometriert"],
    table_error=yaml_config["project"]["table_error"],
    routen_plz=yaml_config["project"]["table_routen_plz"],
    table_view=yaml_config["project"]["table_view"],
    url_wr=yaml_config["url"]["wr_url_walter_route"],
    url_dr=yaml_config["url"]["wr_url_direct_route"],
    error_rate_toleration=yaml_config["project"]["error_rate_toleration"],
    batch_size=yaml_config["project"]["batch_size"],
    folder_name=yaml_config["project"]["folder_name"],
    excel_file_name=yaml_config["project"]["excel_file_name"],
    direct_route_view=yaml_config["project"]["direct_route_view"],
    bucket_name=yaml_config["project"]["bucket_name"],
    file_name=yaml_config["project"]["file_name"],
    user=yaml_config["project"]["user"],
    password=yaml_config["project"]["password"]
)

CONFIG = {**yaml_config, **env_config}

PROJECT_ID = CONFIG.get("PROJECT_ID")
DATASET_ID = CONFIG.get("DATASET_ID")
BASE_COORS = CONFIG.get("TABLE_BASE_COORS")
BASE_WR_KILOMETRIERT = CONFIG.get("TABLE_BASE_COORS_WR_KILOMETRIERT")
ERROR = CONFIG.get("TABLE_ERROR")
ROUTEN_PLZ = CONFIG.get("ROUTEN_PLZ")
TABLE_VIEW = CONFIG.get("TABLE_VIEW")
URL_WR = CONFIG.get("URL_WR")
URL_DR = CONFIG.get("URL_DR")
ERROR_RATE_TOL = CONFIG.get("ERROR_RATE_TOL")
BATCH_SIZE = CONFIG.get("BATCH_SIZE")
FOLDER_NAME = CONFIG.get("FOLDER_NAME")
EXCEL_FILE_NAME = CONFIG.get("EXCEL_FILE_NAME")
CREDENTIALS_PATH = CONFIG.get("CREDENTIALS_PATH")
DIRECT_ROUTE_VIEW = CONFIG.get("DIRECT_ROUTE_VIEW")
BUCKET_NAME = CONFIG.get("BUCKET_NAME")
FILE_NAME = CONFIG.get("FILE_NAME")
USER = CONFIG.get("USER")
PASSWORD = CONFIG.get("PASSWORD")