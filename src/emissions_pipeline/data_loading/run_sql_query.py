import os
from google.cloud import bigquery


client = bigquery.Client()

sql_folder_path = 'path/to/your/sql_folder'

def execute_all_sql_queries(folder_path, project_id, dataset_id):
    for filename in os.listdir(folder_path):
        if filename.endswith('.sql'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as sql_file:
                sql_query = sql_file.read()

                try:
                    print(f"Executing query from file: {filename}")
                    query_job = client.query(sql_query)
                    result = query_job.result()  # Wait for the job to complete                    
                    print(f"Query from {filename} executed successfully.")
                    
                    for row in result:
                        print(row)
                    
                except Exception as e:
                    print(f"Error executing query from {filename}: {e}")

if __name__ == '__main__':
    # Replace with your project and dataset details
    project_id = 'your_project_id'
    dataset_id = 'your_dataset_id'

    # Run all SQL queries in the specified folder
    execute_all_sql_queries(sql_folder_path, project_id, dataset_id)
