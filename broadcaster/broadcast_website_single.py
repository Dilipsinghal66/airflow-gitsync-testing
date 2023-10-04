from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_website_single_jobs import broadcast_website_single

broadcast_website_single_cron = str(
    Variable.get("broadcast_website_single_cron", '@once'))

broadcast_website_single_dag = DAG(
    dag_id="broadcast_website_single",
    default_args=default_args,
    schedule_interval=broadcast_website_single_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_website_single_task = PythonOperator(
    task_id="broadcast_website_single",
    task_concurrency=1,
    python_callable=broadcast_website_single,
    dag=broadcast_website_single_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
