from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from config import local_tz, default_args
from broadcaster.broadcast_pfizer_pregnancy_job import broadcast_pfizer_pregnancy

broadcast_pfizer_pregnancy_cron = str(Variable.get(
    "broadcast_pfizer_pregnancy_cron", '0 12 * * 1'))  # Every Monday at 12 PM

broadcast_pfizer_pregnancy_dag = DAG(
    dag_id="broadcast_pfizer_pregnancy",
    default_args=default_args,
    schedule_interval=broadcast_pfizer_pregnancy_cron,
    catchup=False,
    start_date=datetime(year=2023, month=1, day=2, hour=12, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),  # Adjust to the appropriate Monday
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_pfizer_pregnancy_task = PythonOperator(
    task_id="broadcast_pfizer_pregnancy",
    task_concurrency=1,
    python_callable=broadcast_pfizer_pregnancy,
    dag=broadcast_pfizer_pregnancy_dag,
    op_kwargs={},
    retry_exponential_backoff=True
)
