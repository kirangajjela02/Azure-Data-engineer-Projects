from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pymssql import connect
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import os

# Function to extract data from MS SQL Server
def extract_data_from_mssql(**kwargs):
    server = ''
    user = ''
    password = '1234'
    database = 'project'
    query = 'SELECT * FROM dbo.orders'

    conn = connect(server=server, user=user, password=password, database=database)
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Save DataFrame to CSV
    csv_file_path = '/tmp/data.csv'
    df.to_csv(csv_file_path, index=False)
    return csv_file_path

# Function to store data to Azure Data Lake Gen2
def store_data_to_azure(**kwargs):
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='extract_data')

    storage_account_name = 'adlssss001'
    storage_account_key = ''
    container_name = 'project-datasets'
    blob_name = 'data.csv'

    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=storage_account_key
    )
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    with open(csv_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'mssql_to_azure_datalake',
    default_args=default_args,
    description='Extract data from MS SQL Server and store to Azure Data Lake Gen2',
    schedule_interval='@daily',
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_mssql,
    provide_context=True,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data_to_azure,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
extract_task >> store_task
