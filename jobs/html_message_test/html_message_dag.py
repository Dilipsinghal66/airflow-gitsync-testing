from datetime import datetime
from jobs.html_message_test.html_message_test_job import send_test_message
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from common.pyjson import PyJSON
from airflow.models import Variable
import json

html_test_message_dag_cron = str(Variable.get("html_test_message_dag_cron", '@yearly'))

html_test_message_dag = DAG(
    dag_id="html_test_message_dag",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=html_test_message_dag_cron,
    catchup=False
)

bridge_automation_task = PythonOperator(
    task_id="html_test_message_dag",
    task_concurrency=1,
    python_callable=send_test_message,
    dag=html_test_message_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True,
    provide_context=True
)
