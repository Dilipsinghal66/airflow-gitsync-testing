from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_single_offer_jobs import broadcast_single_offer

broadcast_single_offer_cron = str(Variable.get(
    "broadcast_single_offer_cron", '@yearly'))

broadcast_single_offer_dag = DAG(
    dag_id="broadcast_single_offer",
    default_args=default_args,
    schedule_interval=broadcast_single_offer_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_single_offer_task = PythonOperator(
    task_id="broadcast_single_offer",
    task_concurrency=1,
    python_callable=broadcast_single_offer,
    dag=broadcast_single_offer_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
