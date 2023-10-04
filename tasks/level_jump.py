from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import level_jump_patient
from config import local_tz, default_args

level_jump_dag = DAG(
    dag_id="level_jump",
    default_args=default_args,
    schedule_interval="@every_5_minutes",
    catchup=False,
    start_date=datetime(year=2019, month=7, day=13, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(seconds=15),
)

level_jump_task = PythonOperator(
    task_id="level_jump_task",
    task_concurrency=1,
    python_callable=level_jump_patient,
    dag=level_jump_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
