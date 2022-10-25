# Airflow DAG Object
from airflow import DAG

# Airflow Operators
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Dependencies
import pendulum
from datetime import timedelta
import pyspark_scripts as pyspark

with DAG(
    dag_id='load_and_normalize_data_headers',
    start_date=pendulum.now(),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    },
    catchup=True
) as dag:
    

    load_jsonl_data = PythonOperator(
        task_id='load_and_normalize_jsonlines',
        python_callable=pyspark.load_and_normalize_json_data,
        op_kwargs={
            'json_path': '/opt/airflow/dags/json_storage/'
        }
    )    


    load_parquet_data = PythonOperator(
        task_id='load_and_normalize_parquet',
        python_callable=pyspark.load_and_normalize_parquet_data,
        op_kwargs={
            'parquet_path':'/opt/airflow/dags/parquet_storage/'
        }
    )


    load_rdbms_data = PythonOperator(
        task_id='load_and_normalize_rdbms_data',
        python_callable=pyspark.load_and_normalize_rdbms_data,
        op_kwargs={}
    )


    trigger_data_unification = TriggerDagRunOperator(
        task_id='trigger_data_unification_dag',
        trigger_dag_id='unify_transform_enrich_data'
    )

    load_jsonl_data >> load_parquet_data >> load_rdbms_data >> trigger_data_unification