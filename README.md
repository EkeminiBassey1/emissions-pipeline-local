# Emissions-pipeline-on-prem version

## Authors

- Development Lead - Ekemini Bassey bassey@walter-group.com


## Summary

This project facilitates on-premises calculation for the emissions pipeline. In this README, 
you will find detailed instructions for setting up the project, along with the necessary configurations to ensure successful execution.


## Pre-Prep 

To set up this project, ensure that both Python and Virtualenv are installed. For Windows operating systems, the virtual environment can be set up as follows:

- Install Virtualenv: pip install virtualenv venv
- Activate the virtual environment: venv\Scripts\activate

## Changing the config.yaml

The config.yaml file specifies which projects will undergo calculation. Below are the parameters available for configuration:
- project_id: Identifier for the project.
- dataset_id: Identifier for the dataset. If the dataset does not exist, it will be created automatically using the name specified in the configuration file.
- table_base_coors: Specifies the table from which data is retrieved and assigns an ID to each route.
- table_base_coors_wr_kilometriert: Destination table where calculation results will be stored.
- table_routen_plz: Input table corresponding to the Excel file uploaded into the dataset.
- table_error: Stores area codes that did not yield a route.
- table_view: Provides a view of the base_coors_kilometriert table.
- batch_size: Determines the batch size for each request.
- error_rate_toleration: Defines the permissible error rate as a percentage.
- folder_name: Specifies the folder location for saving multiple Excel files when processing large volumes of area codes requiring conversion.
- excel_file_name: Sets the name for the generated Excel file(s).

This are the attributes in the file, you can change the attributes according to your wishes and run the project

## Importing Key file

To store the calculation in the desired project, please copy the service account key into the "key_file" folder to ensure the necessary permissions for creating and pushing to the project.

## Run the project

With the following command: python main.py --input_file_path in_put_path/folder/name_of_file.xlsx  --output_file_path out_put_path/folder/. A name for the output folder is determined 
in the config.yaml file.