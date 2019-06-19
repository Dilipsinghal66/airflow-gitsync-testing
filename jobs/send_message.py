from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import send_twilio_message, check_message_keys
from config import local_tz, twilio_args

send_twilio_message_dag = DAG(
    dag_id="send_twilio_message",
    default_args=twilio_args,
    schedule_interval=timedelta(seconds=5),
    catchup=False,
    start_date=datetime(year=2019, month=6, day=18, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)

)

twilio_check_message_task = PythonOperator(
    task_id="check_twilio_message",
    task_concurrency=1,
    python_callable=check_message_keys,
    dag=send_twilio_message_dag,
    pool="send_message_pool"
)

twilio_send_message_task = PythonOperator(
    task_id="send_twilio_message",
    task_concurrency=1,
    python_callable=send_twilio_message,
    dag=send_twilio_message_dag,
    pool="send_message_pool",
    retry_exponential_backoff=True,
    depends_on_past=True
)


twilio_send_message_task.set_upstream(twilio_check_message_task)