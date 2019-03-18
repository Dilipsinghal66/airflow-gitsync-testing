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

task_medical_arg_6_00 = Variable.get("task_medical_arg_6_00", deserialize_json=True)
task_exercise_arg_7_30 = Variable.get("task_exercise_arg_7_30", deserialize_json=True)
task_nutrition_arg_9_30 = Variable.get("task_nutrition_arg_9_30", deserialize_json=True)
task_nutrition_arg_13_00 = Variable.get("task_nutrition_arg_13_00", deserialize_json=True)
task_nutrition_arg_17_00 = Variable.get("task_nutrition_arg_17_00", deserialize_json=True)
task_meditation_arg_22_00 = Variable.get("task_meditation_arg_22_00", deserialize_json=True)

medical_dag_6_00 = DAG(
    dag_id='medical_reminder_6_00',
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False
)

exercise_dag_7_30 = DAG(
    dag_id='exercise_reminder_7_30',
    default_args=default_args,
    schedule_interval="30 7 * * *",
    catchup=False
)

nutrition_dag_9_30 = DAG(
    dag_id='nutrition_reminder_9_30',
    default_args=default_args,
    schedule_interval="30 9 * * *",
    catchup=False
)

nutrition_dag_13_00 = DAG(
    dag_id='nutrition_reminder_13_00',
    default_args=default_args,
    schedule_interval="0 13 * * *",
    catchup=False
)

nutrition_dag_17_00 = DAG(
    dag_id='nutrition_reminder_17_00',
    default_args=default_args,
    schedule_interval="0 17 * * *",
    catchup=False
)

meditation_dag_22_00 = DAG(
    dag_id='meditation_reminder_22_00',
    default_args=default_args,
    schedule_interval="0 22 * * *",
    catchup=False
)

medical_reminder_6_00 = PythonOperator(
    task_id="medical_6_30",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=medical_dag_6_00,
    op_kwargs=task_medical_arg_6_00,
    pool="task_reminder_pool"
)

exercise_reminder_7_30 = PythonOperator(
    task_id="exercise_7_30",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=exercise_dag_7_30,
    op_kwargs=task_exercise_arg_7_30,
    pool="task_reminder_pool"
)

nutrition_reminder_9_30 = PythonOperator(
    task_id="nutrition_9_30",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=nutrition_dag_9_30,
    op_kwargs=task_nutrition_arg_9_30,
    pool="task_reminder_pool"
)

nutrition_reminder_13_00 = PythonOperator(
    task_id="nutrition_13_00",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=nutrition_dag_13_00,
    op_kwargs=task_nutrition_arg_13_00,
    pool="task_reminder_pool"
)

nutrition_reminder_17_00 = PythonOperator(
    task_id="nutrition_17_00",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=nutrition_dag_17_00,
    op_kwargs=task_nutrition_arg_17_00,
    pool="task_reminder_pool"
)

meditation_reminder_22_00 = PythonOperator(
    task_id="meditation_22_00",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=meditation_dag_22_00,
    op_kwargs=task_meditation_arg_22_00,
    pool="task_reminder_pool"
)
