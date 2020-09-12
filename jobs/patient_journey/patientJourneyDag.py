from datetime import datetime
from jobs.patient_journey.patientJourneyJob import initializer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from common.pyjson import PyJSON
from airflow.models import Variable
import json



patient_journey_dag = DAG(
    dag_id="patient_journey_dag",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="0 0 * * *",
    catchup=False
)

patient_journey_task = PythonOperator(
    task_id="patient_journey_task",
    task_concurrency=1,
    python_callable=initializer,
    dag=patient_journey_dag,
    op_kwargs={"time": "8:30 AM"},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True
)
