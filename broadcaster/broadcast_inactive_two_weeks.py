from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_inactive_two_weeks_jobs import broadcast_inactive_two_weeks

broadcast_inactive_two_weeks_cron = str(Variable.get(
    "broadcast_inactive_two_weeks_cron", '@once'))

broadcast_inactive_two_weeks_dag = DAG(
    dag_id="broadcast_inactive_two_weeks",
    default_args=default_args,
    schedule_interval=broadcast_inactive_two_weeks_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_inactive_two_weeks_task = PythonOperator(
    task_id="broadcast_inactive_two_weeks",
    task_concurrency=1,
    python_callable=broadcast_inactive_two_weeks,
    dag=broadcast_inactive_two_weeks_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
