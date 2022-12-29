from airflow import DAG
from datetime import datetime, timedelta
import time

from airflow.operators.python_operator import PythonOperator

# ====================================== DAG Config ======================================

default_args = {
    'owner': 'dummy-email@gmail.com',
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': True, 
    'retries': 10,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    "dummy-dags", 
    description='dummy dags for demo',
    schedule_interval="@daily",
    start_date=datetime(2022, 12, 28, 0, 0, 0), 
    catchup=True,
    default_args=default_args
)

# ====================================== Task Function ======================================

def extract_func():

    for i in range(10):
        print( f'do extract function {i}' )
        time.sleep(1)

def transform_func():

    for i in range(10):
        print( f'do extract transform {i}' )
        time.sleep(1)

# ====================================== Task Definition ======================================

extract = PythonOperator(
    task_id='extract', 
    python_callable=extract_func, 
    dag=dag
)

transform = PythonOperator(
    task_id='transform', 
    python_callable=transform_func, 
    dag=dag
)

# ====================================== Depedency Manager ======================================

extract >> transform