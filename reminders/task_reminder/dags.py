from datetime import timedelta

import urllib3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from urllib3.exceptions import InsecureRequestWarning

from reminders.task_reminder.reminder import send_reminder

urllib3.disable_warnings(InsecureRequestWarning)

default_args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(2),
    'depends_on_past': False,
    'email': 'mrigesh@zyla.in',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

auto_dag_def = Variable.get("auto_dag", deserialize_json=True)
for dag_def in auto_dag_def:
    dag_name = dag_def.get("dag_name")
    locals()[dag_name] = DAG(
        dag_id=dag_def.get("dag_id"),
        default_args=default_args,
        schedule_interval=dag_def.get("schedule_interval"),
        catchup=False
    )
    task_name = dag_def.get("task_name")
    locals()[task_name] = PythonOperator(
        task_id=dag_def.get("task_id"),
        task_concurrency=1,
        python_callable=send_reminder,
        dag=locals().get(dag_name),
        op_kwargs=dag_def.get("op_kwargs"),
        pool="task_reminder_pool"
    )
