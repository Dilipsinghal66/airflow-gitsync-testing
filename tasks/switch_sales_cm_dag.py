from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import add_sales_cm, remove_sales_cm
from config import local_tz, default_args

switch_sales_cm_dag = DAG(
    dag_id="switch_sales_cm",
    default_args=default_args,
    schedule_interval="@every_5_minutes",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

add_sales_cm_task = PythonOperator(
    task_id="add_sales_cm",
    task_concurrency=1,
    python_callable=add_sales_cm,
    dag=switch_sales_cm_dag,
    op_kwargs={"cm_type": "sales"},
    
    retry_exponential_backoff=True
)

remove_sales_cm_task = PythonOperator(
    task_id="remove_sales_cm",
    task_concurrency=1,
    python_callable=remove_sales_cm,
    dag=switch_sales_cm_dag,
    op_kwargs={"cm_type": "sales"},
    
    retry_exponential_backoff=True
)
