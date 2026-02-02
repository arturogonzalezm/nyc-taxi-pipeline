"""
DAG for Bronze Layer - Taxi Trip Data Ingestion
Handles: --taxi-type, --start-year, --start-month, --end-year, --end-month
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="taxi_ingestion_dag",
    default_args=default_args,
    description="Ingest taxi trip data from NYC TLC to Bronze layer",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion", "taxi"],
    params={
        "taxi_type": "yellow",
        "start_year": "2025",
        "start_month": "1",
        "end_year": "2025",
        "end_month": "11",
    },
) as dag:

    ingest_taxi_data = BashOperator(
        task_id="ingest_taxi_data",
        bash_command=(
            "docker exec nyc-taxi-etl python -m etl.jobs.bronze.taxi_ingestion_job "
            "--taxi-type {{ params.taxi_type }} "
            "--start-year {{ params.start_year }} "
            "--start-month {{ params.start_month }} "
            "--end-year {{ params.end_year }} "
            "--end-month {{ params.end_month }}"
        ),
    )
