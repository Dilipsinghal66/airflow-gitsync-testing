from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_cc_algo_job import broadcast_cc_algo

broadcast_cc_algo_cron = str(Variable.get("broadcast_cc_algo_cron", '@once'))

broadcast_cc_algo_dag = DAG(
    dag_id="broadcast_cc_algo",
    default_args=default_args,
    schedule_interval=broadcast_cc_algo_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_cc_algo_task = PythonOperator(
    task_id="broadcast_cc_algo",
    task_concurrency=1,
    python_callable=broadcast_cc_algo,
    dag=broadcast_cc_algo_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)