from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_user_ontrial_job import broadcast_newuser_ontrial

broadcast_newuser_ontrial_cron = str(Variable.get("broadcast_newuser_ontrial_cron", '0 8 * * *'))

broadcast_newuser_ontrial_dag = DAG(
    dag_id="broadcast_newuser_ontrial",
    default_args=default_args,
    schedule_interval=broadcast_newuser_ontrial_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_newuser_ontrial_task = PythonOperator(
    task_id="broadcast_newuser_ontrial",
    task_concurrency=1,
    python_callable=broadcast_newuser_ontrial,
    dag=broadcast_newuser_ontrial_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
