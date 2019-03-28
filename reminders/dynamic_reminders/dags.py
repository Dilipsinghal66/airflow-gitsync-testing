from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

from config import local_tz, default_args
from reminders.dynamic_reminders.reminder import send_reminder

dynamic_reminder_21_45_dag = DAG(
  dag_id="dynamic_reminder_21_45",
  default_args=default_args,
  schedule_interval="45 21 * * *",
  catchup=False,
  start_date=dates.days_ago(0).replace(tzinfo=local_tz)
)

dynamic_reminder_21_45_task = PythonOperator(
  task_id="dynamic_reminder_21_45_task",
  task_concurrency=1,
  python_callable=send_reminder,
  dag=dynamic_reminder_21_45_dag,
  op_kwargs={},
  pool="task_reminder_pool",
  retry_exponential_backoff=True,
  provide_context=True
)
