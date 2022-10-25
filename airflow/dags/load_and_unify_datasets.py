# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Dependencies
import pendulum
from datetime import timedelta
from pyspark_scripts import load_and_unify, transform_data, load_and_enrich_data


with DAG(
    dag_id='unify_transform_enrich_data',
    start_date=pendulum.now(),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    },
    catchup=True
) as dag:
    
    data_unification = PythonOperator(
        task_id='load_and_unify_data',
        python_callable=load_and_unify,
        op_kwargs={
            'jsonl':'/opt/airflow/dags/storage/jsonlines_temp/',
            'parquet':'/opt/airflow/dags/storage/parquet_temp/',
            'rdbms':'/opt/airflow/dags/storage/rdbms_temp/'
        }
    )

    data_transform = PythonOperator(
        task_id='load_and_transform_unified_data',
        python_callable=transform_data,
        op_kwargs={
            'path':'/opt/airflow/dags/storage/unified_df/'
        }
    )

    
    data_enrichment = PythonOperator(
        task_id='load_and_enrich_transformed_data',
        python_callable=load_and_enrich_data,
        op_kwargs={
            'path':'/opt/airflow/dags/storage/transformed_unified/'
        }
    )

    trigger_data_aggregations = TriggerDagRunOperator(
        task_id='trigger_data_aggregations_dag',
        trigger_dag_id='enriched_data_aggregations'
    )

    data_unification >> data_transform >> data_enrichment >> trigger_data_aggregations