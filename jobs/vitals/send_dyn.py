from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from jobs.vitals.dynjobs import send_dyn_func
from config import local_tz, default_args

send_dyn_interval = str(Variable.get("send_dyn_interval", '0 30 08 * * ?'))

send_dyn_dag = DAG(
    dag_id="send_dyn_func",
    default_args=default_args,
    schedule_interval=send_dyn_interval,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

switch_active_cm_task = PythonOperator(
    task_id="send_dyn_func",
    task_concurrency=1,
    python_callable=send_dyn_func,
    dag=send_dyn_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True
)