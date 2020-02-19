from datetime import datetime
from broadcaster.broadcast_active_male_job import broadcast_active_male
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from airflow.models import Variable

broadcast_active_male_cron = str(Variable.get("broadcast_active_male_cron",
                                              '@yearly'))

broadcast_active_male_dag = DAG(
    dag_id="broadcast_active_male",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=broadcast_active_male_cron,
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
