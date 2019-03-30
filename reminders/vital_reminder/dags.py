from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from reminders.vital_reminder.reminder import send_reminder

vital_reminder_21_00_dag = DAG(
  dag_id="vital_reminder_21_00",
  default_args=default_args,
  schedule_interval="00 21 * * *",
  catchup=False,
  start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
)

vital_reminder_21_00_task = PythonOperator(
  task_id="vital_reminder_21_00_task",
  task_concurrency=1,
  python_callable=send_reminder,
  dag=vital_reminder_21_00_dag,
  op_kwargs={},
  pool="task_reminder_pool",
  retry_exponential_backoff=True,
  provide_context=True
)
