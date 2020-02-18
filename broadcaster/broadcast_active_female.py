from datetime import datetime
from broadcaster.broadcast_active_female import broadcast_active_fm
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz

broadcast_active_female_dag = DAG(
    dag_id="broadcast_active_female_dag",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval='@once',
    catchup=False
)

broadcast_active_female_task = PythonOperator(
    task_id="broadcast_active_female_task",
    task_concurrency=1,
    python_callable=broadcast_active_fm,
    dag=broadcast_active_female_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
)
