from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.vitals.meditation_reminder_job import meditation_reminder_func

meditation_reminder_interval = str(
    Variable.get("meditation_reminder_interval", '45 21 * * *'))

meditation_reminder_dag = DAG(
    dag_id="meditation_reminder",
    default_args=default_args,
    schedule_interval=meditation_reminder_interval,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

meditation_reminder_task = PythonOperator(
    task_id="meditation_reminder",
    task_concurrency=1,
    python_callable=meditation_reminder_func,
    dag=meditation_reminder_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
