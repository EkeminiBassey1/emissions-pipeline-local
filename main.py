import logging
from src.emissions_pipeline.data_loading.loading_into_tables import DataPrep
import os

def main():
    try:
        input_file_path = input("Please insert the path of the file that needs to be processed: ")
        output_file_path = input("Please specify the path where your file will be stored: ")
        
        try:
            data_prep = DataPrep(input_file_path=input_file_path, output_file_path=output_file_path)        
            data_prep.loading_into_tables()
        
        except Exception as e:
            logging.error(f"An error occurred during data processing: {e}", exc_info=True)
            print("An error occurred during data processing. Please check the error_log.txt for details.")

    except Exception as e:
        logging.error(f"An error occurred while starting the application: {e}", exc_info=True)
        print("An error occurred while starting the application. Please check the error_log.txt for details.")

if __name__ == "__main__":
    main()