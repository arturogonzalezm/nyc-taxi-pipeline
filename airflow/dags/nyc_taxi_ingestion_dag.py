"""
NYC Taxi Data Ingestion DAG
Orchestrates the ingestion of NYC Yellow and Green Taxi trip data into MinIO bronze layer.

This DAG supports both scheduled and manual runs with parameters:
- Scheduled runs: Uses execution_date for year/month
- Manual runs: Uses DAG params (year, month, taxi_types)
"""
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# Add pyspark jobs to Python path
sys.path.insert(0, '/opt/airflow/pyspark')

from jobs.ingestion.taxi_ingestion_job import run_ingestion


def ingest_taxi_data(taxi_type: str, **context):
    """
    Wrapper function to run taxi data ingestion job.

    Args:
        taxi_type: Type of taxi (yellow or green)
        context: Airflow context with execution_date and params
    """
    params = context.get('params', {})

    # Use params if provided, otherwise use execution_date
    if params.get('year') and params.get('month'):
        year = params['year']
        month = params['month']
    else:
        execution_date = context['execution_date']
        year = execution_date.year
        month = execution_date.month

    return run_ingestion(taxi_type, year, month)


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition with parameters
with DAG(
    'nyc_taxi_ingestion',
    default_args=default_args,
    description='Ingest NYC Taxi trip data to MinIO bronze layer',
    schedule_interval='@monthly',  # Run monthly, or trigger manually with params
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['nyc-taxi', 'ingestion', 'pyspark', 'bronze'],
    params={
        'year': Param(
            default=None,
            type=['null', 'integer'],
            description='Year to ingest (e.g., 2023). If not set, uses execution_date.',
            title='Year'
        ),
        'month': Param(
            default=None,
            type=['null', 'integer'],
            description='Month to ingest (1-12). If not set, uses execution_date.',
            title='Month'
        ),
        'taxi_types': Param(
            default=['yellow', 'green'],
            type='array',
            description='List of taxi types to ingest (yellow, green)',
            title='Taxi Types',
            enum=['yellow', 'green']
        ),
    },
    render_template_as_native_obj=True,
) as dag:

    def should_run_task(taxi_type: str, **context) -> bool:
        """Check if this taxi type should be ingested based on params"""
        params = context.get('params', {})
        taxi_types = params.get('taxi_types', ['yellow', 'green'])
        return taxi_type in taxi_types

    # Task: Ingest Yellow Taxi data
    ingest_yellow_taxi = PythonOperator(
        task_id='ingest_yellow_taxi_data',
        python_callable=ingest_taxi_data,
        op_kwargs={'taxi_type': 'yellow'},
        provide_context=True,
    )

    # Task: Ingest Green Taxi data
    ingest_green_taxi = PythonOperator(
        task_id='ingest_green_taxi_data',
        python_callable=ingest_taxi_data,
        op_kwargs={'taxi_type': 'green'},
        provide_context=True,
    )

    # Define task dependencies (run in parallel)
    [ingest_yellow_taxi, ingest_green_taxi]
