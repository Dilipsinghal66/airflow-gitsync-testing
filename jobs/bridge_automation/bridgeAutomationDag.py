from datetime import datetime
from jobs.bridge_automation.bridgeAutomationJob import initializer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from common.pyjson import PyJSON
from airflow.models import Variable
import json



bridge_automation_dag = DAG(
    dag_id="bridge_automation_dag",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="0 0 * * *",
    catchup=False
)

bridge_automation_task = PythonOperator(
    task_id="bridge_automation_dag",
    task_concurrency=1,
    python_callable=initializer,
    dag=bridge_automation_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True,
    provide_context=True
)