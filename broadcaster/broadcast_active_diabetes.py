from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_active_diabetes_job import broadcast_active_diabetes

broadcast_active_diabetes_cron = str(Variable.get(
    "broadcast_active_diabetes_cron", '@yearly'))

broadcast_active_diabetes_dag = DAG(
    dag_id="broadcast_active_diabetes",
    default_args=default_args,
    schedule_interval=broadcast_active_diabetes_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_active_diabetes_task = PythonOperator(
    task_id="broadcast_active_diabetes",
    task_concurrency=1,
    python_callable=broadcast_active_diabetes,
    dag=broadcast_active_diabetes_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
