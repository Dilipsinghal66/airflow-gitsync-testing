from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import switch_active_cm
from config import local_tz, default_args

switch_active_cm_dag = DAG(
    dag_id="switch_active_cm",
    default_args=default_args,
    schedule_interval="@every_5_minutes",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

switch_active_cm_task = PythonOperator(
    task_id="switch_active_cm",
    task_concurrency=1,
    python_callable=switch_active_cm,
    dag=switch_active_cm_dag,
    op_kwargs={"cm_type": "active"},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
