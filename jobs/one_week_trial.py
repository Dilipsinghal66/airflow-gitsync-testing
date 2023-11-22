from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.one_week_trial_job import one_week_trial

one_week_trial_cron = str(Variable.get("one_week_trial_cron", '00 20 * * *'))

one_week_trial_dag = DAG(
    dag_id="one_week_trial",
    default_args=default_args,
    schedule_interval=one_week_trial_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

one_week_trial_task = PythonOperator(
    task_id="one_week_trial",
    task_concurrency=1,
    python_callable=one_week_trial,
    dag=one_week_trial_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)