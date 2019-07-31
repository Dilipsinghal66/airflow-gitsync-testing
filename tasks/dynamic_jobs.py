from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.db_functions import get_data_from_db
from config import local_tz, default_args

scheduled_jobs = get_data_from_db(conn_id="mongo_user_db",
                                  collection="job_storage")

for job in scheduled_jobs:
    job_name = job.get("jobName", "")
    if job_name:
        job_name = job_name.lower().replace(" ", "_")
    else:
        job_name = "random_unnamed_job"
    job_name_dag = job_name + "_dag"
    job_name_task = job_name + "_task"
    dag = DAG(
        dag_id=job_name_dag,
        default_args=default_args,
        schedule_interval="@every_5_minutes",
        catchup=False,
        start_date=datetime(year=2019, month=7, day=31, hour=0, minute=0,
                            second=0,
                            microsecond=0, tzinfo=local_tz),
        dagrun_timeout=timedelta(seconds=15)
    )
    globals()[job_name_task] = PythonOperator(
        task_id=job_name_task,
        task_concurrency=1,
        python_callable=print,
        dag=dag,
        op_kwargs=job,
        pool="dynamic_tasks",
        retry_exponential_backoff=True
    )
