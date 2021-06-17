from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_website_page_jobs import broadcast_website_page

broadcast_website_page_cron = str(
    Variable.get("broadcast_website_page_cron", '@once'))

broadcast_website_page_dag = DAG(
    dag_id="broadcast_website_page",
    default_args=default_args,
    schedule_interval=broadcast_website_page_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_website_page_task = PythonOperator(
    task_id="broadcast_website_page",
    task_concurrency=1,
    python_callable=broadcast_website_page,
    dag=broadcast_website_page_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
