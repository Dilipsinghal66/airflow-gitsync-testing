from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_super_query_jobs import broadcast_super_query

broadcast_super_query_cron = str(Variable.get("broadcast_super_query_cron", '@once'))

broadcast_super_query_dag = DAG(
    dag_id="broadcast_super_query",
    default_args=default_args,
    schedule_interval=broadcast_super_query_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_super_query_task = PythonOperator(
    task_id="broadcast_super_query",
    task_concurrency=1,
    python_callable=broadcast_super_query,
    dag=broadcast_super_query_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
