from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.success_2020.broadcast_2020_job import broadcast_2020

broadcast_2020_cron = str(Variable.get("broadcast_2020_cron", '@yearly'))

broadcast_2020_dag = DAG(
    dag_id="broadcast_2020",
    default_args=default_args,
    schedule_interval=broadcast_2020_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_2020_task = PythonOperator(
    task_id="broadcast_2020",
    task_concurrency=1,
    python_callable=broadcast_2020,
    dag=broadcast_2020_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
