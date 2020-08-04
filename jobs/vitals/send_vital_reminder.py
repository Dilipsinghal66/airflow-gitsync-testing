from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.vitals.vital_reminder import send_vital_reminder_func

vital_reminder_interval = str(
    Variable.get("vital_reminder_interval", '0 07 * * *'))

send_vital_reminder_dag = DAG(
    dag_id="send_vital_reminder_func",
    default_args=default_args,
    schedule_interval=vital_reminder_interval,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

switch_active_cm_task = PythonOperator(
    task_id="send_vital_reminder_func",
    task_concurrency=1,
    python_callable=send_vital_reminder_func,
    dag=send_vital_reminder_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
