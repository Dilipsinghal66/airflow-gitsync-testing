from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_user_device_detail_jobs import broadcast_user_device_detail

broadcast_user_device_details_cron = str(Variable.get("broadcast_user_device_details_cron",
                                          '@once'))

broadcast_device_detail_user_dag = DAG(
    dag_id="broadcast_user_device_detail",
    default_args=default_args,
    schedule_interval=broadcast_user_device_details_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_device_detail_user = PythonOperator(
    task_id="broadcast_user_device_detail",
    task_concurrency=1,
    python_callable=broadcast_user_device_detail,
    dag=broadcast_device_detail_user_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
