import click
import yaml

from src.emissions_pipeline.data_loading.base_coors_data_loading import Base_Coors
from src.emissions_pipeline.data_loading.data_bigquery_upload import update_base_area_code_kilometrierung_table


@click.command()
@click.option("-k", "--key_file", help="Location of the key file to use", type=str, required=True)
def main(key_file):
    uploading_base_coors = Base_Coors()
    project_data = yaml.safe_load(open('config.yaml'))
    
    credential_template = project_data['credentials']['credential']
    credentials = eval(credential_template.replace("key_path", f'"{key_file}"'))

    uploading_base_coors.loading_bq_table_base_coors()
    update_base_area_code_kilometrierung_table(PROJECT_ID=project_data['project']['project_id'],
                                            DATASET_ID=project_data['project']['dataset_id'],
                                            TABLE_BASE_COORS=project_data['project']['table_base_coors'],
                                            TABLE_BASE_COORS_WR_KILOMETRIERT=project_data['project']['table_base_coors_wr_kilometriert'],
                                            TABLE_ERROR=project_data['project']['error_base_coors_wr_kilometriert'],
                                            CREDENTIALS=credentials,
                                            chunk_size=project_data['project']["batch_size"],
                                            url=project_data['url']["wr_url_walter_route"]
                                            )

if __name__ == "__main__":
    main()