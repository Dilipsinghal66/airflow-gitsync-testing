from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import get_distinct_care_managers
from config import local_tz, default_args

cleanup_care_managers_dag = DAG(
    dag_id="cleanup_care_managers",
    default_args=default_args,
    schedule_interval="@every_5_minutes",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

get_distinct_cm = PythonOperator(
    task_id="get_distinct_cm",
    task_concurrency=1,
    python_callable=get_distinct_care_managers,
    dag=cleanup_care_managers_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
