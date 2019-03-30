from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from reminders.task_reminder.reminder import send_reminder

auto_dag_def = Variable.get("auto_dag", deserialize_json=True)
for dag_def in auto_dag_def:
  dag_name = dag_def.get("dag_name")
  locals()[dag_name] = DAG(
    dag_id=dag_def.get("dag_id"),
    default_args=default_args,
    schedule_interval=dag_def.get("schedule_interval"),
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
  )
  task_name = dag_def.get("task_name")
  locals()[task_name] = PythonOperator(
    task_id=dag_def.get("task_id"),
    task_concurrency=1,
    python_callable=send_reminder,
    dag=locals().get(dag_name),
    op_kwargs=dag_def.get("op_kwargs"),
    pool="task_reminder_pool",
    retry_exponential_backoff=True,
    provide_context=True
  )
