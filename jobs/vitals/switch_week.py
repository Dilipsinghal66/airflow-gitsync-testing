from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.vitals.switchweekjob import week_switch

switch_week_interval = str(Variable.get("switch_week_interval", '0 0 * * SUN'))

switch_week_dag = DAG(
    dag_id="switch_week_dag",
    default_args=default_args,
    schedule_interval=switch_week_interval,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

switch_week_task = PythonOperator(
    task_id="switch_week",
    task_concurrency=1,
    python_callable=week_switch,
    dag=switch_week_dag,
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True
)
