from datetime import datetime
from jobs.webinar_reminder.webinarReminderJob import initializer,interaktJob
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from common.pyjson import PyJSON
from airflow.models import Variable
import json


webinar_reminder_dag = DAG(
    dag_id="webinar_reminder_dag",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="00 12 * * *",
    catchup=False
)

webinar_reminder_task = PythonOperator(
    task_id="webinar_reminder_dag",
    task_concurrency=1,
    python_callable=initializer,
    dag=webinar_reminder_dag,
    op_kwargs={"rType": 1},
    
    retry_exponential_backoff=True,
    provide_context=True
)

webinar_reminder_dag_one_day_before = DAG(
    dag_id="webinar_reminder_dag_one_day_before",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="00 19 * * *",
    catchup=False
)

webinar_reminder_task_one_day_before = PythonOperator(
    task_id="webinar_reminder_task_one_day_before",
    task_concurrency=1,
    python_callable=initializer,
    dag=webinar_reminder_dag_one_day_before,
    op_kwargs={"rType": 2},
    
    retry_exponential_backoff=True,
    provide_context=True
)


webinar_reminder_interakt_dag = DAG(
    dag_id="webinar_reminder_interakt_dag",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="00 12 * * *",
    catchup=False
)

webinar_reminder_interakt_task = PythonOperator(
    task_id="webinar_reminder_interakt_dag",
    task_concurrency=1,
    python_callable=interaktJob,
    dag=webinar_reminder_interakt_dag,
    
    retry_exponential_backoff=True,
    provide_context=True
)

webinar_reminder_evening_interakt_dag = DAG(
    dag_id="webinar_reminder_evening_interakt_dag",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="15 18 * * *",
    catchup=False
)

webinar_reminder_evening_interakt_task = PythonOperator(
    task_id="webinar_reminder_evening_interakt_dag",
    task_concurrency=1,
    python_callable=interaktJob,
    dag=webinar_reminder_evening_interakt_dag,
    
    retry_exponential_backoff=True,
    provide_context=True
)