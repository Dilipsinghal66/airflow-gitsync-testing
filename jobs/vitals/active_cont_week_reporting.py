from datetime import datetime
from jobs.vitals.activeContWeekReportingJob import active_week_reporting
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert


active_cont_week_reporting_dag = DAG(
    dag_id="active_cont_week_reporting",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval= '@yearly',
    catchup=False
)

sync_doctor_code_task = PythonOperator(
    task_id="active_cont_week_reporting",
    task_concurrency=1,
    python_callable=active_week_reporting,
    dag=active_cont_week_reporting_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True,
    on_failure_callback=task_failure_email_alert
)
