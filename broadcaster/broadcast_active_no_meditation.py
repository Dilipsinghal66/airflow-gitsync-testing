from datetime import datetime
from broadcaster.broadcast_active_no_meditation import broadcast_active_no_med
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz

broadcast_active_no_meditation_dag = DAG(
    dag_id="broadcast_active_no_meditation_dag",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval='@once',
    catchup=False
)

broadcast_active_no_meditation_task = PythonOperator(
    task_id="broadcast_active_no_meditation_task",
    task_concurrency=1,
    python_callable=broadcast_active_no_med,
    dag=broadcast_active_no_meditation_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
)
