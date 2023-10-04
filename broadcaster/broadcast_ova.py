from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_ova_jobs import broadcast_ova

broadcast_ova_cron = str(Variable.get(
    "broadcast_ova_cron", '@once'))

broadcast_ova_dag = DAG(
    dag_id="broadcast_ova",
    default_args=default_args,
    schedule_interval=broadcast_ova_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_ova_task = PythonOperator(
    task_id="broadcast_ova",
    task_concurrency=1,
    python_callable=broadcast_ova,
    dag=broadcast_ova_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
