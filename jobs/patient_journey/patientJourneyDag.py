from datetime import datetime
from jobs.patient_journey.patientJourneyJob import initializer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from common.alert_helpers import task_failure_email_alert
from common.pyjson import PyJSON
from airflow.models import Variable
import json



patient_journey_dag_8_30 = DAG(
    dag_id="patient_journey_dag_8_30_am",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="30 8 * * *",
    catchup=False
)

patient_journey_task_8_30 = PythonOperator(
    task_id="patient_journey_dag_8_30_am",
    task_concurrency=1,
    python_callable=initializer,
    dag=patient_journey_dag_8_30,
    op_kwargs={"time": "8:30 AM"},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True
)

patient_journey_dag_7_00 = DAG(
    dag_id="patient_journey_dag_7_00_pm",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="0 19 * * *",
    catchup=False
)

patient_journey_task_7_00 = PythonOperator(
    task_id="patient_journey_dag_7_00_pm",
    task_concurrency=1,
    python_callable=initializer,
    dag=patient_journey_dag_7_00,
    op_kwargs={"time": "7:00 PM"},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True
)

patient_journey_dag_9_45 = DAG(
    dag_id="patient_journey_dag_9_45_pm",
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval="45 21 * * *",
    catchup=False
)

patient_journey_task_9_45 = PythonOperator(
    task_id="patient_journey_dag_9_45_pm",
    task_concurrency=1,
    python_callable=initializer,
    dag=patient_journey_dag_9_45,
    op_kwargs={"time": "9:45 PM"},
    pool="scheduled_jobs_pool",
    retry_exponential_backoff=True,
    provide_context=True
)