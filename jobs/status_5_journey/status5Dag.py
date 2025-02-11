from datetime import datetime
from jobs.status_5_journey.status5Job import initializer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from common.pyjson import PyJSON
from airflow.models import Variable
import json


status_5_dag = DAG(
    dag_id="status_5_dag",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="00 20 * * *",
    catchup=False
)

status_5_task = PythonOperator(
    task_id="status_5_dag",
    task_concurrency=1,
    python_callable=initializer,
    dag=status_5_dag,
    
    retry_exponential_backoff=True,
    provide_context=True
)
