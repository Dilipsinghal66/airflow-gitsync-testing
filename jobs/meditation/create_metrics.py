from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.meditation.metricjobs import create_meditation_metrics

create_meditation_interval = str(
    Variable.get("create_meditation_interval", '0 0 * * *'))

create_metric_dag = DAG(
    dag_id="create_meditation_metrics",
    default_args=default_args,
    schedule_interval=create_meditation_interval,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

switch_active_cm_task = PythonOperator(
    task_id="create_meditation_metrics",
    task_concurrency=1,
    python_callable=create_meditation_metrics,
    dag=create_metric_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
