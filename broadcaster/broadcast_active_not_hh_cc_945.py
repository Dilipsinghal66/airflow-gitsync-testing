from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_active_not_hh_cc_945_jobs import broadcast_active_not_hh_cc_945

broadcast_active_not_hh_cc_945_cron = str(Variable.get(
    "broadcast_active_not_hh_cc_945_cron", '@yearly'))

broadcast_active_not_hh_cc_945_dag = DAG(
    dag_id="broadcast_active_not_hh_cc_945",
    default_args=default_args,
    schedule_interval=broadcast_active_not_hh_cc_945_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_active_not_hh_cc_945_task = PythonOperator(
    task_id="broadcast_active_not_hh_cc_945",
    task_concurrency=1,
    python_callable=broadcast_active_not_hh_cc_945,
    dag=broadcast_active_not_hh_cc_945_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
