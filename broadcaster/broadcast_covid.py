from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_covid_jobs import broadcast_covid

broadcast_covid_cron = str(Variable.get("broadcast_covid_cron", '@once'))

broadcast_covid_dag = DAG(
    dag_id="broadcast_covid",
    default_args=default_args,
    schedule_interval=broadcast_covid_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_covid_task = PythonOperator(
    task_id="broadcast_active_hypertension",
    task_concurrency=1,
    python_callable=broadcast_covid,
    dag=broadcast_covid_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
