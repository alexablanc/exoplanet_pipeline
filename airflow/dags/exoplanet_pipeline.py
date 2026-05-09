"""
Airflow DAG: NASA Exoplanet Pipeline
Orchestrates data ingestion, transformation, and quality checks.

Schedule: Daily at 2 AM UTC
Owner: ablanc
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from scripts.fetch_nasa_data import nasa_exoplanet
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Resolve project root dynamically so the DAG works on any machine.
# This file lives at: <project_root>/airflow/dags/exoplanet_pipeline.py
PROJECT_ROOT = '/Users/nopassword/Documents/Portfolio/exoplanets'

default_args = {
    'owner': 'ablanc',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}
# a dag is like the equivalent of a .yml file in ado orchestration where it holds the arguments needed to connect
dag = DAG(
    'exoplanet_pipeline',
    default_args=default_args, # each task can override these if you want to
    description='NASA Exoplanet Archive data pipeline with dbt transformations',
    schedule='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2026, 4, 12),
    catchup=False,
    tags=['ablanc', 'nasa', 'exoplanet'],
)

def fetch_nasa_data(**context):
  #fetch exoplanet data from nasa api. Pulls data from api and saves it to a csv file. Gets the record count from len(data) and uses it in downstream tasks
    sys.path.insert(0, PROJECT_ROOT)

    client = nasa_exoplanet()
    data = client.fetch_exoplanets() #ps_default is the most up to date table called planetary system default table

    output_path = '/Users/nopassword/Documents/Portfolio/exoplanets/data/raw/exoplanets.csv'
    client.save_to_file(data, output_path)

    context['task_instance'].xcom_push(
        key='record_count',
        value=len(data)
    )

def validate_raw_data(**context):
   #validate data before transformations

    raw_file = '/Users/nopassword/Documents/Portfolio/exoplanets/data/raw/exoplanets.csv'
    df = pd.read_csv(raw_file)

    required_columns = [
        'kepler_name',
        'koi_period',
        'koi_teq',
        'koi_prad',
        'koi_smass',
        'koi_disposition',
        'koi_score',
    ]

# check for required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # check row count
    if len(df) == 0:
        raise ValueError("No data rows found in raw file")

    # check for nulls
    important_cols = ['kepler_name', 'koi_disposition']
    null_counts = df[important_cols].isnull().sum()

    context['task_instance'].xcom_push(
        key='validated_rows',
        value=len(df)
    )

fetch_task = PythonOperator(
    task_id='fetch_nasa_data',
    python_callable=fetch_nasa_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data,
    dag=dag,
)

# Task group: dbt transformations
with TaskGroup('dbt_transformations', dag=dag) as dbt_group:

    dbt_run = BashOperator(
        task_id='dbt_run',
        dag=dag,
        bash_command=f'''
            cd {PROJECT_ROOT} && \
            dbt run --profiles-dir dbt --project-dir dbt --select state:modified+ 2>&1
        ''',
        env={
            'DBT_PROFILES_DIR': f'{PROJECT_ROOT}/dbt',
            'DBT_PROJECT_DIR': f'{PROJECT_ROOT}/dbt',
        }
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        dag=dag,
        bash_command=f'''
            cd {PROJECT_ROOT} && \
            dbt test --profiles-dir dbt --project-dir dbt 2>&1
        ''',
        env={
            'DBT_PROFILES_DIR': f'{PROJECT_ROOT}/dbt',
            'DBT_PROJECT_DIR': f'{PROJECT_ROOT}/dbt',
        }
    )

    # dbt docs generate
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        dag=dag,
        bash_command=f'''
            cd {PROJECT_ROOT} && \
            dbt docs generate --profiles-dir dbt --project-dir dbt 2>&1
        ''',
        env={
            'DBT_PROFILES_DIR': f'{PROJECT_ROOT}/dbt',
            'DBT_PROJECT_DIR': f'{PROJECT_ROOT}/dbt',
        }
    )

    # Task dependencies within dbt_group
    dbt_run >> dbt_test >> dbt_docs

# DAG dependencies
fetch_task >> validate_task >> dbt_group
