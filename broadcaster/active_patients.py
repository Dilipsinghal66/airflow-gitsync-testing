from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.active_patients_jobs import active_patients

active_patients_cron = str(Variable.get("active_patients_cron", '@yearly'))

active_patients_dag = DAG(
    dag_id="active_patients",
    default_args=default_args,
    schedule_interval=active_patients_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

active_patients_task = PythonOperator(
    task_id="active_patients",
    task_concurrency=1,
    python_callable=active_patients,
    dag=active_patients_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
