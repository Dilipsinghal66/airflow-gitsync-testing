from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_city_jobs import broadcast_city

broadcast_city_cron = str(Variable.get(
    "broadcast_city_cron", '@once'))

broadcast_city_dag = DAG(
    dag_id="broadcast_city",
    default_args=default_args,
    schedule_interval=broadcast_city_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_city_task = PythonOperator(
    task_id="broadcast_city",
    task_concurrency=1,
    python_callable=broadcast_city,
    dag=broadcast_city_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
