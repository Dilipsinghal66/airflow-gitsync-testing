from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.force_trial_message_job import force_trial_message

force_trial_message_cron = str(Variable.get("force_trial_message_cron", '0 * * * *'))

force_trial_message_dag = DAG(
    dag_id="force_trial_message",
    default_args=default_args,
    schedule_interval=force_trial_message_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

force_trial_message_task = PythonOperator(
    task_id="force_trial_message",
    task_concurrency=1,
    python_callable=force_trial_message,
    dag=force_trial_message_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)