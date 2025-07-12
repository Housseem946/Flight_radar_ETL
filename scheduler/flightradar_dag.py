from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajouter le path du dossier Flight_radar_ETL pour pouvoir importer les modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'etl')))

from extract import extract_data
from transform import transform_data
from load import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='flightradar_etl_job',
    default_args=default_args,
    description='ETL job pour flightradar24',
    schedule_interval='0 */2 * * *',  # Toutes les 2 heures
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flightradar'],
)

t1 = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

t1 >> t2 >> t3
