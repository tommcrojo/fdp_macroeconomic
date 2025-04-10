from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.main_etl import run_full_etl_process

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# List of countries to process
COUNTRIES = [
    'US', 'GB', 'DE', 'JP', 'BR', 'IN', 'ZA',  # Major economies
    'FR', 'CA', 'CN', 'IT', 'RU', 'AU', 'ES',  # Additional major economies
    'MX', 'KR', 'ID', 'TR', 'SA', 'AR'         # Emerging markets
]

def run_etl(**context):
    """
    Execute the ETL process for economic indicators
    """
    # Get the execution date from the context
    execution_date = context['execution_date']
    
    # Calculate the start year (5 years before the execution date)
    start_year = execution_date.year - 5
    
    # Run the ETL process
    result = run_full_etl_process(
        countries=COUNTRIES,
        start_year=start_year,
        initialize_db=False,  # Don't initialize DB on every run
        drop_tables=False     # Don't drop tables on every run
    )
    
    if result['status'] != 'success':
        raise Exception(f"ETL process failed: {result['message']}")
    
    return result

# Create the DAG
dag = DAG(
    'economic_indicators_etl',
    default_args=default_args,
    description='Extract and process economic indicators from World Bank',
    schedule_interval='0 0 1 * *',  # Run monthly on the 1st day
    start_date=days_ago(1),
    catchup=False,
    tags=['economic', 'world_bank', 'etl'],
)

# Task to ensure the logs directory exists
create_logs_dir = BashOperator(
    task_id='create_logs_dir',
    bash_command='mkdir -p /home/tommcrojo/Portfolio_Projects/fdp_macroeconomic/logs',
    dag=dag,
)

# Task to ensure the data directory exists
create_data_dir = BashOperator(
    task_id='create_data_dir',
    bash_command='mkdir -p /home/tommcrojo/Portfolio_Projects/fdp_macroeconomic/data/economic_indicators/raw',
    dag=dag,
)

# Task to run the ETL process
run_etl_task = PythonOperator(
    task_id='run_etl_process',
    python_callable=run_etl,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
[create_logs_dir, create_data_dir] >> run_etl_task
