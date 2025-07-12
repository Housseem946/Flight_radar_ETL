from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract import extract_data
from transform import transform_data
from load import load_data

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flightradar_etl',
    default_args=default_args,
    description='ETL Flightradar24 toutes les 2h',
    schedule_interval='0 */2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

t1 >> t2 >> t3


#  lancer l’analyse Spark automatiquement juste après le load_data :

# from airflow.operators.bash_operator import BashOperator

# t4 = BashOperator(
#     task_id='run_spark_analysis',
#     bash_command='python /chemin/vers/ton/projet/spark_analysis.py',
#     dag=dag,
# )

# t1 >> t2 >> t3 >> t4
