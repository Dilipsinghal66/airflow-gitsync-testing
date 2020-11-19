from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_active_insulin_job import broadcast_active_insulin

broadcast_active_insulin_cron = str(Variable.get(
    "broadcast_active_insulin_cron", '@yearly'))

broadcast_active_insulin_dag = DAG(
    dag_id="broadcast_active_insulin",
    default_args=default_args,
    schedule_interval=broadcast_active_insulin_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_active_insulin_task = PythonOperator(
    task_id="broadcast_active_hh",
    task_concurrency=1,
    python_callable=broadcast_active_insulin,
    dag=broadcast_active_insulin_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
