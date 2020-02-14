from datetime import datetime, timedelta
from broadcaster.broadcast_active_male import broadcast_active_male
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz

broadcast_active_male_dag = DAG(
    dag_id="broadcast_active_male_dag",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=timedelta(days=0, seconds=0, microseconds=0,
                                milliseconds=0, minutes=0, hours=12, weeks=0),
    catchup=False
)

broadcast_active_male_task = PythonOperator(
    task_id="broadcast_active_male_task",
    task_concurrency=1,
    python_callable=broadcast_active_male,
    dag=broadcast_active_male_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
)
