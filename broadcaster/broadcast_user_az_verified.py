from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_user_az_verified_jobs import broadcast_user_az_verified

broadcast_user_az_cron = str(Variable.get("broadcast_user_az_cron", '@once'))

broadcast_az_user_dag = DAG(
    dag_id="broadcast_user_az_verified",
    default_args=default_args,
    schedule_interval=broadcast_user_az_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_az_user_verified = PythonOperator(
    task_id="broadcast_az_user_verified",
    task_concurrency=1,
    python_callable=broadcast_user_az_verified,
    dag=broadcast_az_user_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
