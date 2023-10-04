from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.vitals.reportingjobs import create_reportings_func

create_reporting_interval = str(
    Variable.get("create_reporting_interval", '30 18 * * *'))

create_reportings_dag = DAG(
    dag_id="create_reportings_func",
    default_args=default_args,
    schedule_interval=create_reporting_interval,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

switch_active_cm_task = PythonOperator(
    task_id="create_reportings_func",
    task_concurrency=1,
    python_callable=create_reportings_func,
    dag=create_reportings_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
