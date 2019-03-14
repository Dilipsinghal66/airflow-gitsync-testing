from datetime import timedelta

import urllib3
from airflow import DAG
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

dag = DAG(
    dag_id='exercise_reminder',
    default_args=default_args,
    schedule_interval="30 7 * * *",
    catchup=False
)

reminder_7_30 = PythonOperator(
    task_id="reminder_07_30",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=dag,
    op_kwargs={"taskId": 4180, "payload_var": "task_exercise_payload"},
    pool="task_reminder_pool"
)
