from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import send_pending_callback_messages, check_redis_keys_exist
from config import local_tz, twilio_args

send_callback_message_dag = DAG(
    dag_id="send_callback_message",
    default_args=twilio_args,
    schedule_interval=timedelta(seconds=5),
    catchup=False,
    start_date=datetime(year=2019, month=6, day=18, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)

)

twilio_check_message_task = PythonOperator(
    task_id="check_callback_message",
    task_concurrency=1,
    python_callable=check_redis_keys_exist,
    op_kwargs={"pattern": "*_callback"},
    dag=send_callback_message_dag,
    pool="send_message_pool"
)

send_callback_message_task = PythonOperator(
    task_id="send_callback_message",
    task_concurrency=1,
    python_callable=send_pending_callback_messages,
    dag=send_callback_message_dag,
    pool="send_message_pool",
    retry_exponential_backoff=True
)


send_callback_message_task.set_upstream(twilio_check_message_task)
