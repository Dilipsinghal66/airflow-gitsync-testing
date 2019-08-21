from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import refresh_active_user_redis
from config import local_tz, default_args

active_to_redis_dag = DAG(
    dag_id="active_to_redis",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

active_to_redis_task = PythonOperator(
    task_id="active_to_redis_task",
    task_concurrency=1,
    python_callable=refresh_active_user_redis,
    dag=active_to_redis_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
