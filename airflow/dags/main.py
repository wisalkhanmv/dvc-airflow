import pandas as pd
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Define the sources for data extraction
SOURCES = {
    'dawn': 'https://www.dawn.com/',
    'bbc': 'https://www.bbc.com/'
}

# Data extraction function


def extract():
    all_data = []
    for name, source in SOURCES.items():
        response = requests.get(source)
        soup = BeautifulSoup(response.text, 'html.parser')
        tags = soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        data = [(tag.name, tag.get_text(strip=True)) for tag in tags]
        all_data.extend(data)
    return all_data

# Data transformation function


def transform(ti):
    extracted_data = ti.xcom_pull(task_ids='extract_data')
    df = pd.DataFrame(extracted_data, columns=['Tag', 'Content'])
    df['Content'] = df['Content'].str.replace(
        r'\n|\r', ' ', regex=True).str.strip()
    df.to_csv('transformed_data.csv', index=False)


# Data storage function


def load(**kwargs):
    csv_file = 'transformed_data.csv'
    dvc_repo_dir = 'dvc_folder'
    dvc_data_dir = os.path.join(dvc_repo_dir, 'data')
    os.makedirs(dvc_data_dir, exist_ok=True)
    dvc_file_path = os.path.join(dvc_data_dir, csv_file)
    os.rename(csv_file, dvc_file_path)
    os.chdir(dvc_repo_dir)
    os.system('dvc add ' + dvc_file_path)
    os.system('dvc commit')
    os.system('dvc push')


# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 10),
    'email': ['muhammadwisalkhanmv@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mlops_dag',
    default_args=default_args,
    description='DAG for MLOps data handling and versioning',
    schedule_interval=timedelta(days=1),
)

# Task definitions
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

# Task dependencies
extract_task >> transform_task >> load_task
