from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

from config import local_tz, default_args

reporting_reminder_19_00_dag = DAG(
  dag_id="reporting_reminder_19_00",
  default_args=default_args,
  schedule_interval="45 21 * * *",
  catchup=False,
  start_date=dates.days_ago(0).replace(tzinfo=local_tz)
)

reporting_reminder_19_00_task = PythonOperator(
  task_id="reporting_reminder_19_00_task",
  task_concurrency=1,
  python_callable=print,
  dag=reporting_reminder_19_00_dag,
  op_kwargs={},
  pool="task_reminder_pool",
  retry_exponential_backoff=True,
  provide_context=True
)
