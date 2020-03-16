from datetime import datetime
from jobs.az_tracker.azTrackerRaw1Job import initializer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from common.pyjson import PyJSON
from airflow.models import Variable
import json

config_var = Variable.get('az_tracker_raw1_config', None)

if config_var:
    config_var = json.loads(config_var)
    config_obj = PyJSON(d=config_var)
    cron_time = config_obj.time.cron
else:
    raise ValueError("Config variables not defined")

az_tracker_raw1_dag = DAG(
    dag_id="az_tracker_dag",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=cron_time,
    catchup=False
)

az_tracker_raw1_task = PythonOperator(
    task_id="az_tracker_raw1_task",
    task_concurrency=1,
    python_callable=initializer,
    dag=az_tracker_raw1_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True,
    on_failure_callback=task_failure_email_alert
)
