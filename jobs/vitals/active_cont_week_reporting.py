from datetime import datetime
from jobs.vitals.activeContWeekReportingJob import active_week_reporting
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from airflow.models import Variable
import json
from common.pyjson import PyJSON

config_var = Variable.get('cont_weekly_reporting_config', None)

if config_var:
    config_var = json.loads(config_var)
    config_obj = PyJSON(d=config_var)
    cron_time = config_obj.defaults.cron_time

else:
    raise ValueError("Config variables not defined")

active_cont_week_reporting_dag = DAG(
    dag_id="active_cont_week_reporting",
    default_args=default_args,
    start_date=datetime(year=2020, month=3, day=23, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=cron_time,
    catchup=False
)

active_cont_week_reporting_task = PythonOperator(
    task_id="active_cont_week_reporting",
    task_concurrency=1,
    python_callable=active_week_reporting,
    dag=active_cont_week_reporting_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True,
    on_failure_callback=task_failure_email_alert
)
