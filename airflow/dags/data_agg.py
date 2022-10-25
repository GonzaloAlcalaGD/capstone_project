# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

# Dependencies
import pendulum
from datetime import timedelta
from pyspark_scripts import load_and_aggregate

with DAG(
    dag_id='enriched_data_aggregations',
    start_date=pendulum.now(),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    },
    catchup=True
) as dag:

    data_aggregation = PythonOperator(
        task_id='load_and_generate_aggregations',
        python_callable=load_and_aggregate,
        op_kwargs={
            'path': '/opt/airflow/dags/storage/enriched_data'
        }
    )
       

    data_aggregation