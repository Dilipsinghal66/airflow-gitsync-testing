from datetime import datetime
from jobs.lab_whatsapp_reminder import initializer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from common.pyjson import PyJSON
from airflow.models import Variable
import json


lab_whatsapp_dag = DAG(
    dag_id="lab_whatsapp_reminder_dag",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="00 23 * * *",
    catchup=False
)

lab_whatsapp_task = PythonOperator(
    task_id="lab_whatsapp_reminder_dag",
    task_concurrency=1,
    python_callable=initializer,
    dag=lab_whatsapp_dag,
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True
)