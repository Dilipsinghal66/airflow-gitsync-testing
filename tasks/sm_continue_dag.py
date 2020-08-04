from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import continue_statemachine
from config import local_tz, default_args

sm_continue_dag = DAG(
    dag_id="sm_continue",
    default_args=default_args,
    schedule_interval="@every_5_minutes",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

add_sales_cm_task = PythonOperator(
    task_id="continue_sm_task",
    task_concurrency=1,
    python_callable=continue_statemachine,
    dag=sm_continue_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
