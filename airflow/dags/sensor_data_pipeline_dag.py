from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sensor_data_pipeline',
    default_args=default_args,
    description='A DAG to orchestrate sensor data processing',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def start_kafka_producer():
    subprocess.Popen(["python", "/opt/airflow/dags/src/generate_and_send_data.py"])

start_producer = PythonOperator(
    task_id='start_kafka_producer',
    python_callable=start_kafka_producer,
    dag=dag
)

process_data = SparkSubmitOperator(
    task_id='process_data_with_spark',
    application='/opt/airflow/dags/src/process_data.py',
    conn_id='spark_default',
    dag=dag
)

start_producer >> process_data