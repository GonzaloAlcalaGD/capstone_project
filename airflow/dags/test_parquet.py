# Airflow DAG Object
from airflow import DAG

# Airflow Operators
from airflow.operators.email import EmailOperator
from airflow.decorators import task

# Dependencies
import logging
from datetime import timedelta
import pendulum

with DAG(
    dag_id= 'generate_parquet',
    start_date= pendulum.now(),
    schedule_interval= timedelta(minutes=5),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    catchup= False
) as dag:

    @task()
    def print_logging():
        """ Print test """
        return logging.info('Generating parquet')
    

    print_logging()