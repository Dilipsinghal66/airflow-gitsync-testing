from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.bday_message.bday_message_job import bday_message

bday_message_cron = str(Variable.get("bday_message_cron", '00 10 * * *'))

bday_message_dag = DAG(
    dag_id="bday_message",
    default_args=default_args,
    schedule_interval=bday_message_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

bday_message_task = PythonOperator(
    task_id="bday_message_task",
    task_concurrency=1,
    python_callable=bday_message,
    dag=bday_message_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
