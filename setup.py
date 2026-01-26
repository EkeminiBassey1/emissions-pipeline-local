import sys
import os
from cx_Freeze import setup, Executable

def collect_src_files():
    src_files = []
    for root, dirs, files in os.walk("src"):
        for file in files:
            src_files.append((os.path.join(root, file), os.path.join("src", file)))
    return src_files

if getattr(sys, 'frozen', False):
    base_path = sys._MEIPASS
else:
    base_path = os.path.dirname(os.path.abspath(__file__))
    
includes = [
    'google.cloud.error_reporting',
    'google.cloud.bigquery',
    'inflect',
    'pandas_gbq',
]

includefiles = [
    ('config.yaml', 'config.yaml'),
    ('emissions-pipeline-icon.ico', 'emissions-pipeline-icon.ico'),
    ('requirements.txt', 'requirements.txt'),
] + collect_src_files()

excludes = [
    'cx_Freeze', 'pydoc_data', 'setuptools', 'distutils', 'tkinter', 'typeguard', 'opentelemetry.context.contextvars_context'
]

packages = [
    'click',
    'google',
    'google.cloud',
    'google.cloud.bigquery',
    'google.cloud.error_reporting',
    'google.oauth2',
    'loguru',
    'numpy',
    'pandas',
    'pydantic',
    'openpyxl',
    'inflect',
    'pyfiglet',
    'aiohttp',
    'pandas_gbq',
    'typeguard',
]

base = None
if sys.platform == "win32":
    base = "Win32GUI"  

setup(
    name='Emissions-pipeline',
    version='0.1',
    description='Calculating the routes for the emissions',
    options={
        'build_exe': {
            'includes': includes,
            'excludes': excludes,
            'packages': packages,
            'include_files': includefiles,
        }
    },
    executables=[
        Executable(
            'main.py',
            base=base,
            icon='emissions-pipeline-icon.ico',
            target_name='Emissions-pipeline.exe'
        )
    ]
)
