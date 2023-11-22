from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import twilio_cleanup
from config import local_tz, default_args

twilio_cleanup_dag = DAG(
    dag_id="twilio_cleanup",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

twilio_cleanup_task = PythonOperator(
    task_id="twilio_cleanup_task",
    task_concurrency=1,
    python_callable=twilio_cleanup,
    dag=twilio_cleanup_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
