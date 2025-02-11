from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import add_care_manager
from config import local_tz, default_args

populate_cm_dag = DAG(
    dag_id="populate_cm",
    default_args=default_args,
    schedule_interval="@every_5_minutes",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

add_normal_cm_task = PythonOperator(
    task_id="populate_normal_cm",
    task_concurrency=1,
    python_callable=add_care_manager,
    dag=populate_cm_dag,
    op_kwargs={"check_cm_type": "normal"},
    
    retry_exponential_backoff=True
)

add_az_cm_task = PythonOperator(
    task_id="populate_az_cm",
    task_concurrency=1,
    python_callable=add_care_manager,
    dag=populate_cm_dag,
    op_kwargs={"check_cm_type": "az"},
    
    retry_exponential_backoff=True
)
