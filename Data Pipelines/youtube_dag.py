from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import python_operator
from airflow.utils.date import days_ago
from datetime import datetime

from utility import api_connect,process_comments
from main import main

#CREATING/MENTIONING DEFAULT ARGUMENTS
default_args = {

    'Owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,2,28),
    'email': ['anannyamanojawas@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1) 

}

#CREATING OUR DAG
dag = DAG(

    'youtube_dag',
    default_args=default_args,
    description='My first etl code'

)

#RUNNING THE ETL CODE
run_etl = PythonOperator(

    task_id='complete_youtube_etl',
    python_callable=main,
    dag=dag

)

run_etl
