from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.db_functions import get_data_from_db
from common.helpers import process_dynamic_task
from config import local_tz, default_args

scheduled_jobs = get_data_from_db(conn_id="mongo_user_db",
                                  collection="job_storage")


def get_cron_expression(job_timings=None):
    default_time = "@once"
    if not job_timings:
        return default_time
    scheduled_time = job_timings.get("scheduledTime", None)
    scheduled_date = job_timings.get("scheduledDate", None)
    if not scheduled_time:
        return default_time
    time_list = scheduled_time.split(":")
    try:
        if scheduled_date:
            date_list = scheduled_date.split("-")
        else:
            date_list = None
    except Exception as e:
        date_list = None
    cron_expression = time_list[1] + " " + time_list[0]
    if not date_list:
        cron_expression += " * * *"
    else:
        cron_expression += " " + date_list[2] + " " + date_list[1] + " *"
    return cron_expression


for job in scheduled_jobs:
    job_name = job.get("jobName", "")
    job_time = get_cron_expression(job_timings=job)
    if job_name:
        job_name = job_name.lower().replace(" ", "_")
    else:
        job_name = "random_unnamed_job"
    job_name_dag = job_name + "_dag"
    job_name_task = job_name + "_task"
    dag = DAG(
        dag_id=job_name_dag,
        default_args=default_args,
        schedule_interval=job_time,
        catchup=False,
        start_date=datetime(year=2019, month=7, day=31, hour=0, minute=0,
                            second=0,
                            microsecond=0, tzinfo=local_tz),
        dagrun_timeout=timedelta(seconds=15)
    )
    globals()[job_name_task] = PythonOperator(
        task_id=job_name_task,
        task_concurrency=1,
        python_callable=process_dynamic_task,
        dag=dag,
        op_kwargs=job,
        pool="dynamic_tasks_pool",
        retry_exponential_backoff=True
    )
