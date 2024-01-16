import airflow
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from composer.load_csv import load_table_url_csv
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from datetime import datetime

table_id = 'sylvan-plane-408707.dataset_first.demo'
default_args = {
    'owner':'airflow',
    'email_on_failure':False,
    'email_on_retries':False,
    'start_date':days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('loading-Json-from-GCS-to-Bigquery', description='loading-Json-from-GCS-to-Bigquery', schedule_interval=None,default_args=default_args,start_date=datetime(2023, 1, 1), catchup=False)


start = DummyOperator(
    task_id = 'dummy_task_start',
    retries = 0,
    dag=dag
)

load = PythonOperator(
    task_id = 'loading-json-to-BQ',
    python_callable=load_table_url_csv,
    op_args=[table_id]
)
end=BashOperator(
     task_id='dummy_task_end',
     bash_command='echo "Successfully loaded Json File from GCS to BQ"'
 )

start >> load >> end