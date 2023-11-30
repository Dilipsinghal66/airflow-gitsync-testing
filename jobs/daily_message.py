from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.daily_message_jobs import daily_message

daily_message_cron = str(Variable.get("daily_message_cron", '45 21 * * *'))

daily_message_dag = DAG(
    dag_id="daily_message",
    default_args=default_args,
    schedule_interval=daily_message_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=250),
)

daily_message_task = PythonOperator(
    task_id="daily_message",
    task_concurrency=1,
    python_callable=daily_message,
    dag=daily_message_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
