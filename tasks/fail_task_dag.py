from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from config import local_tz, default_args

failure_dag = DAG(
    dag_id="TaskFailureTest",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(year=2019, month=9, day=13, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    end_date=datetime(year=2019, month=10, day=13, hour=0, minute=0, second=0,
                      microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1)
)

failure_task = BashOperator(
    task_id="TaskFailureTestTask",
    dag=failure_dag,
    bash_command="$((1/0))",
    task_concurrency=1
)
