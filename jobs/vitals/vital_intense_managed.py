from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from jobs.vitals.vital_intense_managed_job import vital_intense_managed

vital_intense_managed_cron = str(Variable.get("vital_intense_managed_cron", '0 23 * * *'))

vital_intense_managed_dag = DAG(
    dag_id="vital_intense_managed",
    default_args=default_args,
    schedule_interval=vital_intense_managed_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

vital_intense_managed_task = PythonOperator(
    task_id="vital_intense_managed",
    task_concurrency=1,
    python_callable=vital_intense_managed,
    dag=vital_intense_managed_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)