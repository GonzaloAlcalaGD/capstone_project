# Airflow DAG Object
from airflow import DAG

# Airflow Operators
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

    trigger_data_normalization = TriggerDagRunOperator(
        task_id='trigger_dag_load_and_normalize_data_headers',
        trigger_dag_id='load_and_normalize_data_headers'
    )


    generate_fake_data >> trigger_data_normalization