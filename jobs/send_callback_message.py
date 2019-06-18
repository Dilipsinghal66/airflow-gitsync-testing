from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import send_pending_callback_messages
from config import local_tz, twilio_args

send_callback_message_dag = DAG(
    dag_id="send_callback_message",
    default_args=twilio_args,
    #schedule_interval=timedelta(seconds=30),
    schedule_interval="@once",
    catchup=False,
    start_date=datetime(year=2019, month=6, day=18, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)

)

send_callback_message_task = PythonOperator(
    task_id="send_callback_message",
    task_concurrency=1,
    python_callable=send_pending_callback_messages,
    dag=send_callback_message_dag,
    pool="send_message_pool",
    retry_exponential_backoff=True
)
