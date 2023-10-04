from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.freemium_journey_job import freemium_journey

freemium_journey_cron = str(Variable.get("freemium_journey_cron", '45 21 * * *'))

freemium_journey_dag = DAG(
    dag_id="freemium_journey",
    default_args=default_args,
    schedule_interval=freemium_journey_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

freemium_journey_task = PythonOperator(
    task_id="freemium_journey",
    task_concurrency=1,
    python_callable=freemium_journey,
    dag=freemium_journey_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)