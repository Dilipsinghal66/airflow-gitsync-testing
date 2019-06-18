from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import send_twilio_message
from config import local_tz, default_args

send_message_dag = DAG(
    dag_id="Send Message",
    default_args=default_args,
    schedule_interval=timedelta(seconds=5),
    catchup=False,
    start_date=datetime(year=2019, month=6, day=18, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)

)

twilio_send_message_task = PythonOperator(
    task_id="patients_activated",
    task_concurrency=1,
    python_callable=send_twilio_message,
    dag=send_message_dag,
    pool="send_message_pool",
    retry_exponential_backoff=True
)
