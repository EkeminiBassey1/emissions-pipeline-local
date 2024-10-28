# emissions-pipeline-on-prem

## Authors

- Development Lead - Ekemini Bassey bassey@walter-group.com


## Summary

This project facilitates on-premises calculation for the emissions pipeline. In this README, 
you will find detailed instructions for setting up the project, along with the necessary configurations to ensure successful execution.


# Pre-Prep 

To set up this project, ensure that both Python and Virtualenv are installed. For Windows operating systems, the virtual environment can be set up as follows:

1. Install Virtualenv: pip install virtualenv venv
2. Activate the virtual environment: venv\Scripts\activate

# Changing the config.yaml

The config.yaml file specifies which projects will undergo calculation. Below are the parameters available for configuration:
  project_id: The identifier for the project.
  dataset_id: The identifier for the dataset. If the dataset does not exist, it will be automatically created based on the name provided in the configuration file.
  table_base_coors: Specifies the table from which data is retrieved, adding an ID to each route.
  table_base_coors_wr_kilometriert: The table where the calculation results will be stored.
  table_routen_plz: The input table, corresponding to the Excel file uploaded into the dataset.
  table_error: Stores area codes that did not return a route.
  table_view: Represents a view of the base_coors_kilometriert table.
  batch_size: Defines the batch size for each request.
  error_rate_toleration: Specifies the acceptable error rate as a percentage.
  folder_name: For large volumes of area codes requiring conversion into Excel files, this designates the folder where multiple Excel files will be saved.