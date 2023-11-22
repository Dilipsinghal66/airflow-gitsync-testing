from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_pregnancy_algo_job import broadcast_send_pregnancy_card

broadcast_pregnancy_algo_cron = str(Variable.get("broadcast_pregnancy_algo_job", '00 18 * * WED,SAT'))

broadcast_pregnancy_algo_dag = DAG(
    dag_id="broadcast_pregnancy_algo",
    default_args=default_args,
    schedule_interval=broadcast_pregnancy_algo_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_newuser_whatsapp_task = PythonOperator(
    task_id="broadcast_pregnancy_algo",
    task_concurrency=1,
    python_callable=broadcast_send_pregnancy_card,
    dag=broadcast_pregnancy_algo_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
