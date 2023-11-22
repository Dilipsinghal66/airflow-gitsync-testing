from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_ontrial_jobs import broadcast_ontrial

broadcast_ontrial_cron = str(Variable.get("broadcast_ontrial_cron", '@once'))

broadcast_ontrial_dag = DAG(
    dag_id="broadcast_ontrial",
    default_args=default_args,
    schedule_interval=broadcast_ontrial_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_ontrial_task = PythonOperator(
    task_id="broadcast_ontrial",
    task_concurrency=1,
    python_callable=broadcast_ontrial,
    dag=broadcast_ontrial_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
