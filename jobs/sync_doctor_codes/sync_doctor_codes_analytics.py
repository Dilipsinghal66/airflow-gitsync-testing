from datetime import datetime
from jobs.sync_doctor_codes.sync_doctor_codes_analytics_job import initializer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from airflow.models import Variable
from common.pyjson import PyJSON
import json

config_var = Variable.get('doctor_sync_analytics_config', None)

if config_var:
    config_var = json.loads(config_var)
    config_obj = PyJSON(d=config_var)
    cron_time = config_obj.time.cron
else:
    raise ValueError("Config variables not defined")

sync_doctor_codes_analytics_dag = DAG(
    dag_id="sync_doctor_codes_analytics_dag",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=cron_time,
    catchup=False
)

sync_doctor_code_analytics_task = PythonOperator(
    task_id="sync_doctor_codes_analytics",
    task_concurrency=1,
    python_callable=initializer,
    dag=sync_doctor_codes_analytics_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True,
    provide_context=True,
    on_failure_callback=task_failure_email_alert
)
