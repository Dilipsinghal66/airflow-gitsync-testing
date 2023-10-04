from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_pending_renewal_jobs import broadcast_pending_renewal

broadcast_pending_renewal_cron = str(Variable.get(
    "broadcast_pending_renewal_cron", '@yearly'))

broadcast_pending_renewal_dag = DAG(
    dag_id="broadcast_pending_renewal",
    default_args=default_args,
    schedule_interval=broadcast_pending_renewal_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_pending_renewal_task = PythonOperator(
    task_id="broadcast_pending_renewal",
    task_concurrency=1,
    python_callable=broadcast_pending_renewal,
    dag=broadcast_pending_renewal_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
