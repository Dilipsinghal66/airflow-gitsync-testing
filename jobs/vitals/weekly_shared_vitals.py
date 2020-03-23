from datetime import datetime
from jobs.vitals.weeklyVitalReportJob import get_weekly_vitals
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
import json
from common.pyjson import PyJSON
from airflow.models import Variable

config_var = Variable.get('weekly_vital_shared_config', None)

if config_var:
    config_var = json.loads(config_var)
    config_obj = PyJSON(d=config_var)
    cron_time = config_obj.defaults.cron_time
else:
    raise ValueError("Config variables not defined")

weekly_shared_vitals_dag = DAG(
    dag_id="weekly_shared_vitals",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=cron_time,
    catchup=False
)

weekly_shared_vitals_task = PythonOperator(
    task_id="weekly_shared_vitals",
    task_concurrency=1,
    python_callable=get_weekly_vitals,
    dag=weekly_shared_vitals_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True,
    on_failure_callback=task_failure_email_alert
)
