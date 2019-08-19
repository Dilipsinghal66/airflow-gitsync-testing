from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from jobs.vitals.vital_reminder import send_vital_reminder
from config import local_tz, default_args

send_vital_reminder_dag = DAG(
    dag_id="send_vital_reminder",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

switch_active_cm_task = PythonOperator(
    task_id="send_vital_reminder",
    task_concurrency=1,
    python_callable=send_vital_reminder,
    dag=send_vital_reminder,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)