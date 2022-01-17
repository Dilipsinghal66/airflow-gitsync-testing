from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_patientids_job import broadcast_patientids

broadcast_patientids_cron = str(Variable.get("broadcast_patientids_cron", '@yearly'))

broadcast_patientids_dag = DAG(
    dag_id="broadcast_patientids",
    default_args=default_args,
    schedule_interval=broadcast_patientids_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_patientids_task = PythonOperator(
    task_id="broadcast_patientids",
    task_concurrency=1,
    python_callable=broadcast_patientids,
    dag=broadcast_patientids_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)
