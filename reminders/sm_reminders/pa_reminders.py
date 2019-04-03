from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args

pa_reminder_12_00_dag = DAG(
  dag_id="pa_reminder_12_00",
  default_args=default_args,
  schedule_interval="01 12 * * *",
  catchup=False,
  start_date=datetime(year=2019, month=4, day=3, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
)

pa_reminder_18_00_dag = DAG(
  dag_id="pa_reminder_18_00",
  default_args=default_args,
  schedule_interval="00 18 * * *",
  catchup=False,
  start_date=datetime(year=2019, month=4, day=3, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
)

pa_reminder_12_00_task = PythonOperator(
  task_id="pa_reminder_12_00_task",
  task_concurrency=1,
  python_callable=print,
  dag=pa_reminder_12_00_dag,
  op_kwargs={},
  pool="task_reminder_pool",
  retry_exponential_backoff=True,
  provide_context=True
)

pa_reminder_18_00_task = PythonOperator(
  task_id="pa_reminder_18_00_task",
  task_concurrency=1,
  python_callable=print,
  dag=pa_reminder_18_00_dag,
  op_kwargs={},
  pool="task_reminder_pool",
  retry_exponential_backoff=True,
  provide_context=True
)
