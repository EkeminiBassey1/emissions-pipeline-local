import click
from src.emissions_pipeline.data_loading.loading_into_tables import loading_into_tables


@click.command()
@click.option("-i", "--input_file_path", help="The input file of the area codes", type=str, required=True)
@click.option("-n", "--name_file", help="The name of the file", type=str, required=True)
@click.option("-o", "--output_file_path", help="Location of where the file will be stored", type=str, required=True)
def main(input_file_path, output_file_path, name_file):
    loading_into_tables(input_file_path, output_file_path, name_file)
    
if __name__ == "__main__":
    main()