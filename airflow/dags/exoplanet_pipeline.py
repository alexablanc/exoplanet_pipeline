"""
Airflow DAG: NASA Exoplanet Pipeline
Orchestrates data ingestion, transformation, and quality checks.
Schedule: Daily at 2 AM UTC
Owner: ablanc
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.scripts.fetch_nasa_data import nasa_exoplanet

# Path to the project root inside the Docker container.
# Update this to match the mounted path in your docker-compose.override.yml.
PROJECT_ROOT = '/Users/nopassword/Documents/Portfolio/exoplanets'

# Snowflake Configuration
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_DATABASE = 'EXOPLANETS_DB'
SNOWFLAKE_SCHEMA = 'RAW'
SNOWFLAKE_TABLE = 'RAW_EXOPLANET_DATA'

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
    description='NASA Exoplanet Archive data pipeline with snowflake and dbt transformations',
    schedule='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2026, 4, 12),
    catchup=False,
    tags=['ablanc', 'nasa', 'exoplanet'],
)

def fetch_and_load_data(**context):
    """
    Task: Fetch exoplanet data from the NASA API and load it into Snowflake.

    Responsibilities:
        1. Call the NASA Exoplanet Archive API via the nasa_exoplanet client.
        2. Use SnowflakeHook (Airflow) to create the raw table if it does not exist.
        3. Truncate and reload the table for a full daily refresh.
        4. Push the record count to XCom for downstream tasks.

    Note:
        SnowflakeHook lives here in the DAG — NOT in fetch_nasa_data.py — because
        fetch_nasa_data.py is a pure Python utility with no Airflow dependencies.
    """
    from scripts.fetch_nasa_data import nasa_exoplanet

    # Step 1: Fetch data
    client = nasa_exoplanet()
    data = client.fetch_exoplanets(table="cumulative") #ps_default table has the planetary system data which is more interesting.
    # you would need to add in the columns you want from that table because the default columns are for cumulative.

    if not data:
        raise ValueError("NASA API returned no data. Aborting load.")

    # Step 2: Connect to Snowflake via the Airflow connection
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        # Step 3: Create the raw table if it does not already exist.
        # All columns are VARCHAR — dbt handles type casting in the staging layer.
        columns = list(data[0].keys())
        col_defs = ", ".join([f"{col} VARCHAR" for col in columns])
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} "
            f"({col_defs}, INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
        )

        # Step 4: Full refresh — truncate and reload
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_TABLE}")

        # Step 5: Batch insert all records
        insert_sql = (
            f"INSERT INTO {SNOWFLAKE_TABLE} ({', '.join(columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))})"
        )
        values = [tuple(row.get(col) for col in columns) for row in data]
        cursor.executemany(insert_sql, values)
        conn.commit()

        print(f"Successfully loaded {len(data)} records into "
              f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")

    finally:
        cursor.close()
        conn.close()

    # Push record count for downstream validation
    context["task_instance"].xcom_push(key="record_count", value=len(data))


def validate_snowflake_data(**context):
    """
    Task: Validate that data was successfully loaded into the Snowflake raw table.

    Checks:
        - Row count is greater than zero.
        - Prints null counts for key columns (kepler_name, koi_disposition).
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # Check total row count
    row_count = hook.get_first(
        f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
    )[0]

    if row_count == 0:
        raise ValueError(
            f"Failed validation: No rows found in "
            f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
        )

    # Check null counts in columns
    null_counts = hook.get_first(f"""
        SELECT
            COUNT(CASE WHEN kepler_name IS NULL THEN 1 END)    AS missing_kepler_name,
            COUNT(CASE WHEN koi_disposition IS NULL THEN 1 END) AS missing_disposition
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    """)

    print(f"Validation passed: {row_count} rows loaded.")
    print(f"Null counts — kepler_name: {null_counts[0]}, koi_disposition: {null_counts[1]}")

    context["task_instance"].xcom_push(key="validated_rows", value=row_count)


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

fetch_task = PythonOperator(
    task_id="fetch_and_load_nasa_data",
    python_callable=fetch_and_load_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_snowflake_data",
    python_callable=validate_snowflake_data,
    dag=dag,
)

# Task group: dbt transformations run after raw data is validated in Snowflake
with TaskGroup("dbt_transformations", dag=dag) as dbt_group:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"dbt run --profiles-dir dbt --project-dir dbt 2>&1"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"dbt test --profiles-dir dbt --project-dir dbt 2>&1"
        ),
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"dbt docs generate --profiles-dir dbt --project-dir dbt 2>&1"
        ),
    )

    dbt_run >> dbt_test >> dbt_docs

fetch_task >> validate_task >> dbt_group