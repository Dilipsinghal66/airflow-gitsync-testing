from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_active_without_doccode_jobs import broadcast_active_without_doccode

broadcast_active_without_doccode_cron = str(Variable.get(
    "broadcast_active_without_doccode_cron", '@yearly'))

broadcast_active_without_doccode_dag = DAG(
    dag_id="broadcast_active_without_doccode",
    default_args=default_args,
    schedule_interval=broadcast_active_without_doccode_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_active_without_doccode_task = PythonOperator(
    task_id="broadcast_active_without_doccode",
    task_concurrency=1,
    python_callable=broadcast_active_without_doccode,
    dag=broadcast_active_without_doccode_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
