from datetime import datetime
from broadcaster.broadcast_ios_user_job import broadcast_ios_user
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from airflow.models import Variable

broadcast_ios_user_cron = str(Variable.get("broadcast_ios_user_cron", '@once'))

broadcast_ios_user_dag = DAG(
    dag_id="broadcast_ios_user",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=broadcast_ios_user_cron,
    catchup=False
)

broadcast_ios_user_task = PythonOperator(
    task_id="broadcast_ios_user_task",
    task_concurrency=1,
    python_callable=broadcast_ios_user,
    dag=broadcast_ios_user_dag,
    op_kwargs={},
    
)
