# Airflow DAG Object
from airflow import DAG

# Airflow Operators
from airflow.operators.python import PythonOperator

# Dependencies
from datetime import timedelta
import pendulum
from insertion_orchestration import gen_data

with DAG(
    dag_id= 'generate_data',
    start_date= pendulum.now(),
    schedule_interval= timedelta(minutes=5),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    catchup= False
) as dag:

    generate_fake_data = PythonOperator(
        task_id='generate_data',
        python_callable=gen_data
    )


    generate_fake_data